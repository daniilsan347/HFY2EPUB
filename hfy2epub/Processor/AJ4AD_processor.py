from hfy2epub.Processor.base_processor import BaseProcessor
import os
import re
from datetime import datetime

class AJ4ADProcessor(BaseProcessor):
    """
    Processor for the AJ4AD (A Job for a Deathwolder) series from HFY subreddit.
    This processor handles the processing of chapters.
    """

    def process_chapter(self, chapter_path: str) -> None:
        with open(chapter_path, 'r', encoding='utf-8') as file:
            chapter_text = file.readlines()
        
        self.remove_redundant_links(chapter_text)
        self.replace_delimiter(chapter_text)
        self.remove_title_padding(chapter_text)

        # Chapter one fix
        if 'Chapter one' in chapter_path:
            title_pos = chapter_text.index('**A job for a Deathworlder**\n')
            chapter_text[title_pos] = '# Chapter 1 – A Job for a Deathworlder'
        else:
            title_pos = self.find_chapter_title(chapter_text, chapter_path)
        if title_pos is None:
            print(f"Warning: No title found in chapter {chapter_path}. Skipping processing.")
            return

        self.format_author_notes(chapter_text, title_pos)
        title = chapter_text[title_pos].strip()
        del chapter_text[title_pos]  # Remove the title line from the main text
        chapter_text.insert(0, title + '\n\n')

        self.add_timestamp(chapter_text, chapter_path)
        output_path = os.path.join(self.processed_dir, os.path.basename(chapter_path))
        with open(output_path, 'w', encoding='utf-8') as file:
            file.writelines(chapter_text)

    def remove_redundant_links(self, chapter_text: list[str]) -> None:
        """
        Remove redundant links from the chapter text.
        """
        previous_chapter_pattern = re.compile(r'\[\\\[Chapter 1\\\]\].+')
        next_chapter_pattern = re.compile(r'\[[\\\[]+Next Chapter[\\\]+]+\].*')

        # Remove the previous chapter link
        del chapter_text[0] # For most cases
        for i, line in enumerate(chapter_text): # For oddball cases
            if previous_chapter_pattern.match(line):
                del chapter_text[:i+1]

        for i, line in enumerate(chapter_text):
            if next_chapter_pattern.search(line):
                del chapter_text[i-1:i+1]  # Remove the line before the next chapter link
                break
    
    def replace_delimiter(self, chapter_text: list[str]) -> None:
        """
        Replace the delimiter in the chapter text.
        """
        pattern = re.compile(r'^[-\\]+$')
        for i, line in enumerate(chapter_text):
            if pattern.match(line):
                chapter_text[i] = '-----\n'
    
    def remove_title_padding(self, chapter_text: list[str]) -> None:
        """
        Remove the padding around the title.
        """
        pattern = re.compile(r'&#x200B;')
        i = 0
        while i < len(chapter_text):
            if pattern.match(chapter_text[i]):
                del chapter_text[i-1:i+1] 
                continue
            i += 1

    def find_chapter_title(self, chapter_text: list[str], chapter_path: str) -> int|None:
        """
        Find the chapter title in the chapter text.
        """
        pattern_w_title = re.compile(r'(?:# |\**)?Chapter ([\d/ AB]+) +– +(.+)')
        pattern_wo_title = re.compile(r'(?:# )?Chapter ([\d/]+)')
        fallback_pattern = re.compile(r'\[.+\[(.+)\]\]')
        fallback_pos = 0
        for i, line in enumerate(chapter_text):
            if match := pattern_w_title.match(line.strip().replace('-', '–')):
                chapter_text[i] = f"# Chapter {match.group(1)} – {match.group(2)}"
                chapter_text[i] = chapter_text[i].replace('*', '').replace('**', '')
                print(f'Found title "{chapter_text[i]}" at line {i} in {chapter_path}')
                return i
            elif match := pattern_wo_title.match(line):
                chapter_text[i] = f"# Chapter {match.group(1)}"
                print(f'Found title "{chapter_text[i]}" at line {i} in {chapter_path}')
                return i
        
        for i, line in enumerate(chapter_text):
            if line.strip().startswith('-----'):
                fallback_pos = i
                break

        if match := fallback_pattern.search(chapter_path):
            chapter_text.insert(fallback_pos, f"# {match.group(1)}")
            print(f'Found fallback title "{chapter_text[fallback_pos]}" at line {fallback_pos} in {chapter_path}')
            return fallback_pos
    
    def format_author_notes(self, chapter_text: list[str], title_pos: int) -> None:
        """
        Extract author notes from the chapter text.
        """
        for i in range(title_pos):
            if chapter_text[i].strip().startswith('-----'):
                break 
            if i == title_pos - 1:
                chapter_text[i] = '\n\n-----\n\n'  # Add a delimiter before the title if not present
            chapter_text[i] = '> ' + chapter_text[i]


    def add_timestamp(self, chapter_text: list[str], chapter_path: str) -> None:
        """
        Add a timestamp to the chapter text.
        """
        published_date = [chapter['revision_date'] for chapter in self.raw_metadata['chapters'] if chapter['filename'] == os.path.basename(chapter_path)]
        if published_date:
            published_date = datetime.fromtimestamp(published_date[0]).strftime('%Y-%m-%d %A')
            chapter_text.insert(1, f"**Date**: `{published_date}`\n\n")