import argparse
import os
import sys
from collections import defaultdict
from pathlib import Path

# Add current directory to sys.path to ensure shared_utils can be imported
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from shared_utils import natural_sort_key
except ImportError:
    # Fallback if shared_utils is missing
    import re

    def natural_sort_key(s):
        return [
            int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", str(s))
        ]


def list_works(root_folder, output_file):
    root_path = Path(root_folder)

    if not root_path.exists():
        print(f"Error: The folder '{root_folder}' does not exist.")
        return

    # Dictionary to hold folder -> list of files
    works_by_folder = defaultdict(list)

    # Walk the directory tree
    for root, _, files in os.walk(root_path):
        current_dir = Path(root)

        # Determine relative path for cleaner output
        try:
            relative_folder = current_dir.relative_to(root_path)
        except ValueError:
            relative_folder = Path(".")

        # Filter out hidden files (starting with .)
        visible_files = [f for f in files if not f.startswith(".")]

        if visible_files:
            # Store files associated with this folder path (as string)
            works_by_folder[str(relative_folder)].extend(visible_files)

    # Sort folders naturally
    sorted_folders = sorted(works_by_folder.keys(), key=natural_sort_key)

    try:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(f"Listing works in: {root_path.resolve()}\n\n")

            for folder in sorted_folders:
                file_list = works_by_folder[folder]
                # Sort files naturally
                file_list.sort(key=natural_sort_key)

                # Header for the folder
                folder_display = folder if folder != "." else "(Root)"
                f.write(f"[{folder_display}]\n")

                for filename in file_list:
                    f.write(f"  - {filename}\n")

                f.write("\n")  # Empty line between folders
        print(f"Successfully wrote catalog to: {os.path.abspath(output_file)}")
    except IOError as e:
        print(f"Error writing to file '{output_file}': {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="List every work within the Reading folder, by folder."
    )
    parser.add_argument(
        "folder",
        nargs="?",
        default=r"C:\Users\Miro\Documents\Reading",
        help="Path to the Reading folder (default: Reading)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="library_catalog.txt",
        help="Output text file (default: library_catalog.txt)",
    )

    args = parser.parse_args()
    list_works(args.folder, args.output)
