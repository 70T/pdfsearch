import html
import logging
import os
import re
import aiohttp
import discord
import sys
from discord import app_commands
from discord.ext import commands
from shared_utils import truncate_text, is_folder_allowed, get_instance_lock
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---

PDFSEARCH_BASE = "http://localhost:5001"
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
MATCHES_PER_PAGE = 3
API_TIMEOUT = 15  # per individual HTTP request


# --- Access Control ---

def _parse_id_list(env_val: str) -> set[int]:
    if not env_val:
        return set()
    return {int(x.strip()) for x in env_val.split(",") if x.strip().isdigit()}


ALLOWED_GUILDS: set[int] = _parse_id_list(os.getenv("ALLOWED_GUILDS", ""))
ALLOWED_USERS: set[int] = _parse_id_list(os.getenv("ALLOWED_USERS", ""))
ALLOWED_FOLDERS = [
    "Star Wars",
    "ASOIAF",
    "Warhammer",
]

# Folder Whitelists
# If an ID is present in these maps, the bot restricts searches to these folders.
# Set these manually in code for now or move to a JSON config for complex setups.
GUILD_FOLDER_MAP: dict[int, list[str]] = {
    # 0000000000: ["Star Wars"],
}

USER_FOLDER_MAP: dict[int, list[str]] = {
    # 0000000000: ["Warhammer"],
}


def get_allowed_folders_for_interaction(interaction: discord.Interaction) -> list[str]:
    """Resolves which folders are allowed for the current context."""
    # User-specific overrides take highest priority
    if interaction.user.id in USER_FOLDER_MAP:
        return USER_FOLDER_MAP[interaction.user.id]

    # Guild-specific overrides
    if interaction.guild_id in GUILD_FOLDER_MAP:
        return GUILD_FOLDER_MAP[interaction.guild_id]

    # Fallback to global whitelist
    return ALLOWED_FOLDERS


# --- Logging & Context ---

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# --- Utilities & HTML Conversion ---

# Regex Patterns
RE_HTML_BOLD = re.compile(r"<b>(.*?)</b>", re.DOTALL)
RE_HTML_TAGS = re.compile(r"<[^>]+>")


def _html_to_discord(text: str) -> str:
    if not text:
        return ""
    # 1. Use placeholders for our highlight tags so they don't get escaped
    text = text.replace("<b>", "\x01").replace("</b>", "\x02")
    # 2. Unescape HTML (converts &lt; to <, etc)
    text = html.unescape(text)
    # 3. Escape Markdown special characters
    # Note: escape backslash first to avoid double-escaping
    for char in ["\\", "*", "_", "~", "`", ">", "#"]:
        text = text.replace(char, "\\" + char)
    # 4. Restore our highlight tags as Discord bold
    text = text.replace("\x01", "**").replace("\x02", "**")
    # 5. Remove any remaining HTML tags
    text = RE_HTML_TAGS.sub("", text)
    return text.strip()


# --- API Clients ---

async def fetch_api(bot, endpoint, params=None):
    url = f"{bot.api_base}{endpoint}"
    try:
        async with bot.session.get(
            url, params=params, timeout=aiohttp.ClientTimeout(total=API_TIMEOUT)
        ) as resp:
            if resp.status == 200:
                return await resp.json()
            else:
                text = await resp.text()
                logger.error(f"API Error {resp.status} on {endpoint}: {text[:200]}")
    except Exception as e:
        logger.error(f"API Error on {endpoint}: {e}")
    return None


# --- Discord UI Components ---

class BookSelect(discord.ui.Select):
    def __init__(self, book_page_map):
        options = [
            discord.SelectOption(label=truncate_text(t, 100), value=str(idx))
            for t, idx in list(book_page_map.items())[:25]
        ]
        super().__init__(placeholder="Jump to book...", options=options)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()

        idx = int(self.values[0])
        self.view.current = idx
        self.view._sync_buttons()

        await interaction.edit_original_response(
            embed=self.view.embeds[idx], view=self.view
        )


class PaginationView(discord.ui.View):
    def __init__(self, bot, embeds, book_page_map, page_metadata, query, invoker_id):
        super().__init__(timeout=180)
        self.bot = bot
        self.embeds = embeds
        self.current = 0
        self.page_metadata = page_metadata
        self.query = query
        self.invoker_id = invoker_id
        if len(book_page_map) > 1:
            self.add_item(BookSelect(book_page_map))
        self._sync_buttons()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.invoker_id:
            await interaction.response.send_message(
                "Keep your hands off! These search results belong to someone else. Run `/search` to get your own.",
                ephemeral=True,
            )
            return False
        return True

    def _sync_buttons(self):
        self.prev_button.disabled = self.current == 0
        self.next_button.disabled = self.current == len(self.embeds) - 1

    @discord.ui.button(
        label="Previous", style=discord.ButtonStyle.secondary, custom_id="prev"
    )
    async def prev_button(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        self.current -= 1
        self._sync_buttons()
        await interaction.response.edit_message(
            embed=self.embeds[self.current], view=self
        )

    @discord.ui.button(
        label="Next", style=discord.ButtonStyle.secondary, custom_id="next"
    )
    async def next_button(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        self.current += 1
        self._sync_buttons()
        await interaction.response.edit_message(
            embed=self.embeds[self.current], view=self
        )

    @discord.ui.button(
        label="Get Context", style=discord.ButtonStyle.primary, custom_id="rich"
    )
    async def rich_button(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        await interaction.response.defer(ephemeral=True)

        meta = self.page_metadata[self.current]
        params = {
            "file_id": meta["file_id"],
            "page": meta["page"],
            "q": self.query,
        }

        data = await fetch_api(self.bot, "/snippet", params)

        if data and data.get("snippet"):
            snippet = _html_to_discord(data["snippet"])
            title = meta["filename"]
            page = meta["page"]

            # Construct the deep link to the file viewer
            view_url = f"{self.bot.api_base}/view/{meta['file_id']}#page={page}"

            # Create a nice rich context embed
            embed = discord.Embed(
                title=f"Context: {title}",
                description=snippet,
                color=discord.Color.blurple(),
                url=view_url,
            )
            embed.set_footer(text=f"Page {page}")

            if meta.get("chapter"):
                embed.add_field(name="Chapter", value=meta["chapter"], inline=False)

            # Use a separate follow-up so it's only visible to the clicking user
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            await interaction.followup.send(
                "Could not retrieve extended context for this page.", ephemeral=True
            )


# --- Discord Bot Client ---

class PDFSearchBot(commands.Bot):
    def __init__(self):
        super().__init__(
            command_prefix="!", intents=discord.Intents.default(), help_command=None
        )
        self.api_base = PDFSEARCH_BASE
        self.session = None

    async def setup_hook(self):
        self.session = aiohttp.ClientSession()
        # Register slash commands globally
        for guild_id in ALLOWED_GUILDS:
            guild = discord.Object(id=guild_id)
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)

    async def on_ready(self):
        logger.info(f"Logged in as {self.user} (ID: {self.user.id})")
        logger.info("------")

    async def close(self):
        if self.session:
            await self.session.close()
        await super().close()


bot = PDFSearchBot()


# --- Slash Commands ---

@bot.tree.command(name="search", description="Search the PDF library for a phrase.")
@app_commands.describe(
    query="The phrase to search for (use quotes for exact matches)",
    sort="Sort results by: filename (default) or relevance",
    folders="Optional: specific folders to search in (e.g. Star Wars, ASOIAF)",
)
@app_commands.choices(
    sort=[
        app_commands.Choice(name="Filename", value="filename"),
        app_commands.Choice(name="Relevance", value="relevance"),
    ]
)
async def search(
    interaction: discord.Interaction,
    query: str,
    sort: str = "filename",
    folders: str = None,
):
    # Check if the guild or user is allowed to use the bot
    if (
        interaction.guild_id not in ALLOWED_GUILDS
        and interaction.user.id not in ALLOWED_USERS
    ):
        await interaction.response.send_message(
            "This bot is not authorized for use in this server or by this user.",
            ephemeral=True,
        )
        return

    await interaction.response.defer()

    # Determine which folders to search in
    if folders:
        # User provided specific folders; check if they are allowed
        requested = [f.strip() for f in folders.split(",")]
        allowed = get_allowed_folders_for_interaction(interaction)
        selected = [f for f in requested if is_folder_allowed(f, allowed)]
        if not selected:
            await interaction.followup.send(
                f"None of the requested folders are in your whitelist. Allowed: {', '.join(allowed) or 'All'}",
                ephemeral=True,
            )
            return
    else:
        # Use interaction-specific whitelist (default)
        selected = get_allowed_folders_for_interaction(interaction)

    params = {
        "q": query,
        "limit": 50,
        "sort": sort,
    }
    if selected:
        params["folders"] = ",".join(selected)

    data = await fetch_api(bot, "/search", params)

    if not data or not data.get("results"):
        await interaction.followup.send(
            f"No matches found for: `{query}`", ephemeral=False
        )
        return

    results = data["results"]
    total_books = data.get("total_books", 0)
    total_pages = data.get("total_pages", 0)

    # Build embeds for pagination
    embeds = []
    page_metadata = []  # To store file_id/page for context retrieval
    book_page_map = {}  # To allow jumping to specific books

    for i, (filename, path, matches, book_total) in enumerate(results):
        # Record the start index for this book in the embeds list
        book_page_map[filename] = len(embeds)

        for match in matches:
            embed = discord.Embed(
                title=filename,
                description=_html_to_discord(match["snippet"]),
                color=discord.Color.blue(),
                url=f"{PDFSEARCH_BASE}/view/{match['file_id']}#page={match['page']}",
            )
            embed.set_author(name=f"Search Query: {query}")
            if match.get("chapter"):
                embed.add_field(name="Chapter", value=match["chapter"], inline=False)

            footer_text = f"Result {len(embeds) + 1} | Page {match['page']} of {filename}"
            if book_total > len(matches):
                footer_text += f" ({book_total} total matches in book)"
            embed.set_footer(text=footer_text)

            embeds.append(embed)
            page_metadata.append(
                {
                    "file_id": match["file_id"],
                    "page": match["page"],
                    "filename": filename,
                    "chapter": match.get("chapter"),
                }
            )

    view = PaginationView(
        bot, embeds, book_page_map, page_metadata, query, interaction.user.id
    )
    await interaction.followup.send(
        content=f"Found **{total_pages}** matches across **{total_books}** books.",
        embed=embeds[0],
        view=view,
    )


# --- Application Entry Point ---

if __name__ == "__main__":
    if not DISCORD_TOKEN:
        print("Error: DISCORD_TOKEN not found in environment.")
        sys.exit(1)

    # Ensure only one bot instance runs
    _lock = get_instance_lock("pdfsearch_bot")
    if not _lock:
        print("Error: Another instance of the bot is already running.")
        sys.exit(1)

    try:
        bot.run(DISCORD_TOKEN)
    except KeyboardInterrupt:
        pass
