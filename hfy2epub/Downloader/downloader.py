import praw
import os
from glob import glob
import yaml
from hfy2epub.Downloader.validator import validate_metadata
from hfy2epub.Downloader.validator import compare_meta_and_wiki
from queue import Queue
import pandas as pd

from praw.models.comment_forest import CommentForest
from praw.models import Comment

class Downloader:
    def __init__(self, reddit: praw.Reddit, raw_dir: str, wiki_dir: str) -> None:
        self.reddit = reddit
        self.raw_dir = raw_dir
        self.wiki_dir = wiki_dir

    def run(self) -> None:
        """
        Main method to run the downloader.
        This method will handle the main logic of downloading chapters from the subreddit wiki.
        """
        
        # If the raw directory is invalid, clear it and fetch all chapters again
        if not validate_metadata(self.raw_dir):
            print("[Downloader] Invalid metadata, clearing raw directory and fetching chapters again.")
            metadata = self.fetch_all_chapters()
        else: 
            metadata = self.fetch_update()

        if metadata:
            with open(os.path.join(self.raw_dir, 'metadata.yaml'), 'w', encoding='utf-8') as file:
                yaml.safe_dump(metadata, file, allow_unicode=True)
            print(f"[Downloader] Metadata saved to {os.path.join(self.raw_dir, 'metadata.yaml')}")

    def fetch_update(self) -> dict | None:
        """
        Fetch the latest update from the subreddit wiki if any.
        """
        metadata_file = os.path.join(self.raw_dir, 'metadata.yaml')
        wiki_files = glob(os.path.join(self.wiki_dir, '*.yaml'))
        
        if not wiki_files:
            print("No wiki files found in the wiki directory.")
            return None
        
        wiki_files.sort(key=os.path.getmtime, reverse=True)
        wiki_file = wiki_files[0]
        
        is_valid, missing_chapters = compare_meta_and_wiki(metadata_file, wiki_file)
        if not is_valid:
            print("[Downloader] Metadata does not match the wiki data. Fetching all chapters again.")
            return self.fetch_all_chapters()
        
        if missing_chapters.empty():
            print("[Downloader] No missing chapters found. Metadata is up to date.")
            return None
        
        print(f"[Downloader] Missing chapters found: {missing_chapters.qsize()}. Fetching missing chapters.")
        
        with open(metadata_file, 'r', encoding='utf-8') as file:
            metadata = yaml.safe_load(file)
        with open(wiki_file, 'r', encoding='utf-8') as file:
            wiki_data = yaml.safe_load(file)

        df_chapters = pd.DataFrame(metadata['chapters'])
        df_chapters.sort_values(by='revision_date', ascending=False, inplace=True)
        for chapter in df_chapters['url'][:2]:
            missing_chapters.put(chapter)

        print(f'[Downloader] Starting download of {missing_chapters.qsize()} missing chapters from subreddit "{wiki_data["subreddit"]}".')

        while not missing_chapters.empty():
            chapter_url = missing_chapters.get()
            chapter_metadata = self.fetch_chapter(chapter_url)
            if chapter_metadata:
                metadata['chapters'].append(chapter_metadata)
                print(f"Downloaded chapter: {chapter_metadata['title']} ({chapter_metadata['url']})")
            missing_chapters.task_done()
        
        cleared_metadata = self.delete_old_chapters(metadata['chapters']).to_dict(orient='records')
        metadata['chapters'] = cleared_metadata

        return metadata

    def fetch_all_chapters(self) -> dict | None:
        """
        Fetch all chapters from the subreddit wiki.
        This method will download all chapters and save them in the raw directory.
        """
        for file in glob(os.path.join(self.raw_dir, '*.*')):
            os.remove(file)

        wiki_files = glob(os.path.join(self.wiki_dir, '*.yaml'))
        if not wiki_files:
            print("No wiki files found in the wiki directory.")
            return {}
        
        wiki_files.sort(key=os.path.getmtime, reverse=True)
        wiki_file = wiki_files[0]
        with open(wiki_file, 'r', encoding='utf-8') as file:
            wiki_data = yaml.safe_load(file)
        
        if not wiki_data or 'chapters' not in wiki_data:
            print("No chapters found in the wiki data.")
            return {}
        
        download_queue = Queue()
        for chapter in wiki_data['chapters']: download_queue.put(chapter['url'])

        metadata = {
            'subreddit': wiki_data['subreddit'],
            'wiki_uri': wiki_data['wiki_uri'],
            'wiki_section': wiki_data['wiki_section'],
            'revision_date': wiki_data['revision_date'],
            'chapters': []
        }

        print(f"[Downloader] Starting download of {download_queue.qsize()} chapters from subreddit '{wiki_data['subreddit']}'.")
        while not download_queue.empty():
            chapter_url = download_queue.get()
            chapter_metadata = self.fetch_chapter(chapter_url)
            if chapter_metadata:
                metadata['chapters'].append(chapter_metadata)
                print(f"Downloaded chapter: {chapter_metadata['title']} ({chapter_metadata['url']})")
        
        return metadata

    def fetch_chapter(self, chapter_url: str) -> dict | None:
        """
        Download a chapter from the subreddit wiki.
            :param chapter_url: The URL of the chapter to download.
        """
        try:
            submission = self.reddit.submission(url=chapter_url)

            content_md: list[str] = [submission.selftext]

            comment_forest: CommentForest = submission.comments
            op_replies = self.fetch_op_chain(comment_forest, submission.author.name)
            
            if op_replies:
                for comment in op_replies:
                    content_md.append(comment.body)
            
            if op_replies:
                revision_date = int(op_replies[-1].created_utc)
            else:   
                revision_date = int(submission.created_utc)

            title = submission.title.strip().replace('/', '-').replace('\\', '-')

            file_name = f'{revision_date} - {submission.id} - [{title}].md'
            file_path = os.path.join(self.raw_dir, file_name)
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write('\n\n-----\n\n'.join(content_md))

            metadata = {
                'url': chapter_url,
                'title': submission.title.strip(),
                'filename': file_name,
                'revision_date': revision_date
            }

            return metadata

        except Exception as e:
            print(f"[Downloader] Error downloading chapter {chapter_url}: {e}")
            return
    
    def fetch_op_chain(self, root: CommentForest, op_username: str) -> list[Comment] | None:
        
        def search(current_nodes: CommentForest, path: list[Comment]) -> list | None:
            for comment in current_nodes:
                # Check if the current node has the target attribute
                if comment.author and comment.author.name == op_username:
                    current_path = path + [comment]
                    # Recursively search the children of the current node
                    result = search(comment.replies, current_path)
                    if result:
                        return result
                    else:
                        # If no further chain is found, return the current path if it's not empty
                        return current_path if len(current_path) > 0 else None
                else:
                    # If the node does not have the target attribute, skip its subtree
                    continue
            return None
        return search(root, [])

    def delete_old_chapters(self, chapters: list[dict]) -> pd.DataFrame:
        """
        Delete old versions of chapters based on their URLs and revision dates.
        This method will keep the latest version of each chapter based on the URL and revision date.
            
        :param chapters: List of chapter metadata dictionaries.
        """
        chapters_df = pd.DataFrame(chapters)
        df_sorted = chapters_df.sort_values(by=['url','revision_date'], ascending=False)
        df_filtered = df_sorted.drop_duplicates(subset=['url'], keep='first')
        
        mask = df_sorted['url'].isin(df_filtered['url'])
        df_to_delete = df_sorted[~mask]
        for _, row in df_to_delete.iterrows():
            file_path = os.path.join(self.raw_dir, row['filename'])
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"[Downloader] Deleted old version of a chapter: {row['title']} ({row['url']})")
        
        return df_filtered
