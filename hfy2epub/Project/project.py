import praw
import os
from hfy2epub.Project.project_config import ProjectConfig
from hfy2epub.Project.wiki_processor import WikiProcessor

class Project:
    def __init__(self, config: ProjectConfig):
        self.config = config


        # Initialize Reddit client
        self.reddit = praw.Reddit(
            config.reddit_bot,
            config_interpolation='basic'
        )

        # Ensure project directory exists
        self.project_dir = os.path.join(os.getcwd(), 'projects', config.project_name)
        self.wiki_dir = os.path.join(self.project_dir, 'wiki')
        self.raw_dir = os.path.join(self.project_dir, 'raw')
        self.processed_dir = os.path.join(self.project_dir, 'processed')

        os.makedirs(self.project_dir, exist_ok=True)
        os.makedirs(self.wiki_dir, exist_ok=True)
        os.makedirs(self.raw_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)



    def run(self):
        """
        Main method to run the project.
        This method will handle the main logic of the project.
        """

        wiki_processor = WikiProcessor(
            subreddit=self.reddit.subreddit(self.config.subreddit_name),
            wiki_uri=self.config.wiki_uri,
            wiki_section=self.config.wiki_section
        )
        wiki_processor.fetch_wiki_data()
        wiki_processor.write_wiki_data(self.wiki_dir)

        print(f"Wiki data for subreddit '{self.config.subreddit_name}' has been fetched and written to {self.wiki_dir}.")
        
        pass  # Placeholder for the main logic of the project