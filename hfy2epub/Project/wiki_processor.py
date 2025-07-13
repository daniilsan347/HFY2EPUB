from datetime import datetime
from praw.models import Subreddit, WikiPage
import mistune
import re
import yaml
import os
from datetime import datetime

class WikiProcessor:
    def __init__(self, subreddit: Subreddit, wiki_uri: str, wiki_section: str):
        self.subreddit = subreddit
        self.wiki_section = wiki_section
        self.wiki_uri = wiki_uri


    def fetch_wiki_data(self) -> dict:
        """
        Fetch the wiki data from the subreddit.
        Returns:
            wiki_data: A dictionary containing the subreddit name, wiki URI, wiki section, revision date, and chapters. Chapters are a list of tuples (url, title).
        Raises:
            ValueError: If the wiki page is not found or if no chapters are found.
        """
        
        wiki_page = self.subreddit.wiki[self.wiki_uri]
        if not wiki_page:
            raise ValueError(f"Wiki page '{self.wiki_uri}' not found in subreddit '{self.subreddit.display_name}'.")
        
        chapters = self.extract_chapters(wiki_page)
        if not chapters:
            raise ValueError(f"No chapters found in the wiki section '{self.wiki_section}' of subreddit '{self.subreddit.display_name}'.")
        
        revision_date = datetime.fromtimestamp(wiki_page.revision_date).strftime('%Y-%m-%d')

        self.wiki_data = {
            'subreddit': self.subreddit.display_name,
            'wiki_uri': self.wiki_uri,
            'wiki_section': self.wiki_section,
            'revision_date': revision_date,
            'author': wiki_page.revision_by.name if wiki_page.revision_by else 'Unknown',
            'chapters': [{'url': url, 'title': title} for url, title in chapters]
        }

        return self.wiki_data

    def extract_chapters(self, wiki_page: WikiPage) -> list[tuple[str, str]]:
        """
        Extract chapters from the wiki data.
        
        Returns:
            A list of tuples (url, title).
        """
        renderer = SectionLinkRenderer(self.wiki_section)
        markdown = mistune.create_markdown(renderer=renderer)
        fixed_content = re.sub(r'(^|\s)(#+)(?=[^\s#])', r'\1\2 ', wiki_page.content_md)
        markdown(fixed_content)

        return renderer.links

    def write_wiki_data(self, output_dir: str):
        """
        Write the wiki data to a YAML file.
        
        :param output_file: The path to the output YAML file.
        """
        if not hasattr(self, 'wiki_data'):
            raise ValueError("Wiki data has not been fetched. Call fetch_wiki_data() first.")
        
        output_file = os.path.join(output_dir, f"wiki_{self.subreddit.display_name}_[{self.wiki_section}]_{self.wiki_data['revision_date']}.yaml")

        with open(output_file, 'w', encoding='utf-8') as file:
            yaml.dump(self.wiki_data, file, allow_unicode=True, default_flow_style=False)
        
class SectionLinkRenderer(mistune.HTMLRenderer):
    def __init__(self, section_title):
        super().__init__()
        self.links = []
        self.in_section = False
        self.section_title = section_title

    def strong(self, text):
        return text
    
    def emphasis(self, text):
        return text

    def heading(self, text, level, **attrs):
        if text.strip().lower() == self.section_title.strip().lower():
            self.in_section = True
        else:
            self.in_section = False
        return f'<h{level}>{text}</h{level}>'
    
    def link(self, text, url, title=None):
        if self.in_section:
            # Replace all characters unsupported by most file systems with '_'
            text = re.sub(r'[<>:"\/\\|?*]', '_', text)
            self.links.append((url, text))
        return super().link(url, text, title)