from hfy2epub.Project.project import Project
from hfy2epub.Project.project_config import ProjectConfig

import os

def main():
    # Define the project configuration
    config = ProjectConfig(
        reddit_bot='HFY2EPUB',
        wiki_url='https://www.reddit.com/r/HFY/wiki/series/a_job_for_a_deathworlder/',
        wiki_section='A Job For A Deathworlder',
        project_name='hfy2epub_test_project'
    )

    # Create the project instance
    project = Project(config)

    # Run the project
    project.run()

if __name__ == "__main__":
    main()