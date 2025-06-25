class ProjectConfig:
    def __init__(self, reddit_bot: str, wiki_url: str, wiki_section: str, project_name: str):
        """
        Initialize the project configuration.

        :param reddit_bot: Name of the bot configured in praw.ini config.
        :param project_name: Directory where output files will be saved.
        """
        self.reddit_bot = reddit_bot
        self.project_name = project_name
        self.wiki_section = wiki_section

        uri = wiki_url.split('https://www.reddit.com/r/')[1].strip('/').split('/')
        
        if len(uri) < 3:
            raise ValueError("Invalid wiki URL format. Expected format: https://www.reddit.com/r/{subreddit_name}/wiki/{wiki_uri}")
        
        self.subreddit_name = uri[0]
        self.wiki_uri = '/'.join(uri[2:])

    def __repr__(self):
        return f"ProjectConfig(reddit_bot={self.reddit_bot}, project_name='{self.project_name}')"