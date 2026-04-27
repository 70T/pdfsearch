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

load_dotenv(override=True)

# --- Configuration ---

PDFSEARCH_BASE = os.getenv("PDFSEARCH_BASE", "http://localhost:5001").rstrip("/")
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
CF_CLIENT_ID = os.getenv("CF_CLIENT_ID", "")
CF_CLIENT_SECRET = os.getenv("CF_CLIENT_SECRET", "")
MATCHES_PER_PAGE = 3
API_TIMEOUT = 15  # per individual HTTP request


# --- Access Control ---


def _parse_id_list(env_val: str) -> set[int]:
    if not env_val:
        return set()
    return {
        int(val)
        for x in env_val.split(",")
        if (val := x.strip().strip("'\"")).isdigit()
    }


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
RE_MD_ESCAPE = re.compile(r"([\\*_~`>#])")
RE_BOLD_PLACEHOLDERS = re.compile(r"[\x01\x02]")


def _html_to_discord(text: str) -> str:
    if not text:
        return ""
    # 1. Use placeholders for our highlight tags so they don't get escaped
    text = text.replace("<b>", "\x01").replace("</b>", "\x02")
    # 2. Unescape HTML (converts &lt; to <, etc)
    text = html.unescape(text)
    # 3. Escape Markdown special characters
    text = RE_MD_ESCAPE.sub(r"\\\1", text)
    # 4. Restore our highlight tags as Discord bold
    text = RE_BOLD_PLACEHOLDERS.sub("**", text)
    # 5. Remove any remaining HTML tags
    text = RE_HTML_TAGS.sub("", text)
    return text.strip()


# --- API Clients ---


async def fetch_api(bot, endpoint, params=None):
    url = f"{bot.api_base}{endpoint}"
    headers = {}
    if CF_CLIENT_ID and CF_CLIENT_SECRET:
        headers["CF-Access-Client-Id"] = CF_CLIENT_ID
        headers["CF-Access-Client-Secret"] = CF_CLIENT_SECRET

    try:
        async with bot.session.get(
            url,
            params=params,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=API_TIMEOUT),
        ) as resp:
            if resp.status == 200:
                try:
                    return await resp.json()
                except Exception as je:
                    text = await resp.text()
                    logger.error(
                        f"JSON Decode Error on {endpoint}: {je}. Response starts with: {text[:100]}"
                    )
                    return None
            else:
                text = await resp.text()
                logger.error(f"API Error {resp.status} on {endpoint}: {text[:200]}")
    except Exception as e:
        logger.error(f"API Error on {endpoint}: {e}")
    return None


# --- Discord UI Components ---


class BookSelect(discord.ui.Select):
    def __init__(self, book_page_map):
        # Convert map items to a list to use index-based unique values
        self.ordered_books = list(book_page_map.items())[:25]
        options = [
            discord.SelectOption(label=truncate_text(t, 100), value=str(i))
            for i, (t, idx) in enumerate(self.ordered_books)
        ]
        super().__init__(placeholder="Jump to book...", options=options)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()

        # Look up the page index using the selected index
        book_idx = int(self.values[0])
        _, page_idx = self.ordered_books[book_idx]

        assert self.view is not None, "Select must be attached to a view"
        self.view.current = page_idx
        self.view._sync_buttons()

        await interaction.edit_original_response(
            embed=self.view.embeds[page_idx], view=self.view
        )


class PaginationView(discord.ui.View):
    def __init__(self, embeds, book_page_map, invoker_id):
        super().__init__(timeout=180)
        self.embeds = embeds
        self.current = 0
        self.invoker_id = invoker_id
        if len(book_page_map) > 1:
            self.add_item(BookSelect(book_page_map))
        self._sync_buttons()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.invoker_id:
            await interaction.response.send_message(
                "Keep your hands off! These search results belong to someone else.",
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
        label="Snippets Loaded",
        style=discord.ButtonStyle.primary,
        custom_id="loaded",
        disabled=True,
    )
    async def loaded_button(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        pass


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
        # Register slash commands globally and sync to specific guilds for immediate update
        logger.info(f"Syncing commands for guilds: {ALLOWED_GUILDS}")
        if not ALLOWED_GUILDS:
            logger.warning(
                "No ALLOWED_GUILDS found in environment. Commands may not be available in guilds."
            )

        for guild_id in ALLOWED_GUILDS:
            try:
                guild = discord.Object(id=guild_id)
                self.tree.clear_commands(
                    guild=guild
                )  # Clear specific guild commands to avoid signature duplicates
                self.tree.copy_global_to(guild=guild)
                synced = await self.tree.sync(guild=guild)
                logger.info(f"Synced {len(synced)} commands to guild {guild_id}")
            except Exception as e:
                logger.error(f"Failed to sync commands for guild {guild_id}: {e}")

        # Note: Global sync is omitted here to prevent duplicates in guilds where we manually sync.
        # If you want global commands (with 1-hour propagation), use self.tree.sync() instead of the guild loop above.

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
        "search_query": query,
        "limit": 50,
        "sort_by": sort,
    }
    if selected:
        params["folders"] = ",".join(selected)

    data = await fetch_api(bot, "/api/search", params)

    if not data or not data.get("results"):
        await interaction.followup.send(
            f"No matches found for: `{query}`", ephemeral=False
        )
        return

    results = data["results"]
    total_books = data.get("total_books", 0)
    total_pages = data.get("total_pages", 0)

    # Build text blocks for 3 matches per embed
    page_blocks = []
    current_match_blocks = []
    book_page_map = {}  # For Jump to book select menu

    for entry in results:
        filename = entry["filename"]
        rel_path = entry["relative_path"]
        matches = entry["matches"]
        book_total = entry["match_count"]

        # Record start page for this book
        if filename not in book_page_map:
            book_page_map[filename] = len(page_blocks)

        # Get folder path (e.g. Star Wars/Legends)
        folder = os.path.dirname(rel_path).replace("\\", "/")

        for i, match in enumerate(matches, 1):
            match_header = f"**[{folder}] {filename} ({i}/{book_total})**"
            page_info = f"Page {match['page']}"
            if match.get("chapter"):
                page_info += f" - {match['chapter']}"

            snippet_text = _html_to_discord(match["snippet"])
            quoted_snippet = "\n".join(f"> {line}" for line in snippet_text.split("\n"))

            block = f"{match_header}\n{page_info}\n{quoted_snippet}"
            current_match_blocks.append(block)

            if len(current_match_blocks) >= MATCHES_PER_PAGE:
                page_blocks.append("\n\n".join(current_match_blocks))
                current_match_blocks = []

    if current_match_blocks:
        page_blocks.append("\n\n".join(current_match_blocks))

    if not page_blocks:
        await interaction.followup.send(f"No matches found for: `{query}`")
        return

    # Build final embeds
    embeds = []
    header_info = f'**Searched for:** "{query}"\n**Found:** {total_pages} total match(es) in {total_books} book(s)\n\n'

    for i, p in enumerate(page_blocks):
        footer_text = f"Page {i + 1} of {len(page_blocks)} - PDFSearch bot by miro - check /help for instructions"

        # Grey color to match Discord's background (0x2b2d31 or slightly lighter)
        # Using 0x2c2f33 which is a common Discord dark grey.
        embed = discord.Embed(
            description=f"{header_info}{p}\n\n{footer_text}", color=0x2B2D31
        )
        embeds.append(embed)

    view = PaginationView(embeds, book_page_map, interaction.user.id)
    await interaction.followup.send(
        embed=embeds[0],
        view=view,
    )


@search.autocomplete("folders")
async def search_folders_autocomplete(
    interaction: discord.Interaction, current: str
) -> list[app_commands.Choice[str]]:
    allowed = get_allowed_folders_for_interaction(interaction)
    return [
        app_commands.Choice(name=f, value=f)
        for f in allowed
        if current.lower() in f.lower()
    ][:25]


@bot.tree.command(name="help", description="How to use the PDFSearch bot.")
async def help_cmd(interaction: discord.Interaction):
    help_text = (
        "**PDFSearch Bot**\n\n"
        "This bot is a whitelisted frontend for a private, self-hosted PDF search engine by miro. It does not grant access to any media outside of snippets. Use `/search` to find phrases within the PDF library.\n\n"
        "**Parameters:**\n"
        '`query`: The term or phrase you\'re looking for. Use "double quotes" for exact phrase matches.\n'
        "`sort`: (Optional) Choose to sort by `Filename` or `Relevance`. Default is Filename.\n"
        "`folders`: (Optional) Restrict your search to specific folders (e.g., 'Star Wars'). Autocomplete will help you find allowed folders.\n\n"
        "The bot returns up to 50 results, paginated 3 per embed.\n"
        "Use the 'Jump to book' dropdown to quickly navigate between different files.\n"
        "Only users and servers on the whitelist can use this bot.\n"
        "Jumbled results are caused by OCR noise or poor PDF formatting."
    )
    embed = discord.Embed(
        title="PDFSearch Help",
        description=help_text,
        color=0x2B2D31,
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


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
