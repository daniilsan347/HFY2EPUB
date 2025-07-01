import os
import yaml
from glob import glob
from queue import Queue

def validate_metadata(raw_dir: str) -> bool:
    """
    Compare metadata file and downloaded markdown files.
        :param raw_dir: Directory containing raw metadata files.
        :return: True if all metadata files match the downloaded markdown files, False otherwise.
    """

    metadata_files = glob(os.path.join(raw_dir, '*.yaml'))
    markdown_files = glob(os.path.join(raw_dir, '*.md'))

    if not metadata_files:
        print("No metadata files found.")
        return False

    if not markdown_files:
        print("No markdown files found.")
        return False

    markdown_set: set[str] = set(os.path.basename(f) for f in markdown_files)

    with open(os.path.join(raw_dir, 'metadata.yaml'), 'r', encoding='utf-8') as file:
        metadata = yaml.safe_load(file)
        metadata_set = set()

        for chapter in metadata['chapters']:
            metadata_set.add(chapter['filename'])

    missing_markdown: set[str] = metadata_set - markdown_set

    if len(missing_markdown) > 0:
        print(f"Missing markdown files for chapters: {missing_markdown}")
        return False
    elif len(missing_markdown) < 0:
        print(f"Extra markdown files found: {markdown_set - metadata_set}")
        return False

    return True

def compare_meta_and_wiki(metadata_file: str, wiki_file: str) -> tuple[bool, Queue[str]]:
    """
    Compare metadata file and wiki file.
        :param metadata_file: Path to the metadata YAML file.
        :param wiki_file: Path to the wiki YAML file.
        :return: Tuple containing a boolean indicating if the metadata matches the wiki, and a Queue with missing chapters if any.
    """
    with open(metadata_file, 'r', encoding='utf-8') as meta_file:
        metadata = yaml.safe_load(meta_file)

    with open(wiki_file, 'r', encoding='utf-8') as wiki_f:
        wiki_data = yaml.safe_load(wiki_f)

    if metadata['wiki_uri'] != wiki_data['wiki_uri']:
        print(f"Wiki URI mismatch: {metadata['wiki_uri']} != {wiki_data['wiki_uri']}")
        return False, Queue()
    if metadata['wiki_section'] != wiki_data['wiki_section']:
        print(f"Wiki section mismatch: {metadata['wiki_section']} != {wiki_data['wiki_section']}")
        return False, Queue()
    
    metadata_chapters = {chapter['url']: chapter for chapter in metadata['chapters']}
    wiki_chapters = {chapter['url']: chapter for chapter in wiki_data['chapters']}
    missing_chapters = set(wiki_chapters.keys()) - set(metadata_chapters.keys())

    if missing_chapters:
        print(f"Missing chapters in metadata: {missing_chapters}")
        
        update_queue = Queue()
        for url in missing_chapters:
            update_queue.put(url)
        return True, update_queue
    
    return True, Queue()

    