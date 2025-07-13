import os
from glob import glob
from queue import Queue
import yaml
import pandas as pd

class BaseProcessor:
    """
    Base class for all novel processors.
    """
    
    def __init__(self, raw_dir: str, processed_dir: str):
        self.raw_dir = raw_dir
        self.processed_dir = processed_dir
        self.metadata = {}
        self.raw_metadata = {}
        self.processing_queue = Queue()
    
    def validate_metadata(self) -> bool:
        """
        Validate the metadata file. Checks if the file exists and the content alligns with the directory contents.
        """
        raw_metadata_file = os.path.join(self.raw_dir, 'metadata.yaml')
        metadata_file = os.path.join(self.processed_dir, 'metadata.yaml')
        # Raise an error since this cannot be handled from this class
        if not os.path.exists(raw_metadata_file):
            raise FileNotFoundError(f"Raw metadata file not found: {raw_metadata_file}")
        with open(raw_metadata_file, 'r', encoding='utf-8') as file:
            self.raw_metadata = yaml.safe_load(file)

        if not os.path.exists(metadata_file):
            print(f"[Processor] Metadata file not found: {metadata_file}")
            return False
        with open(metadata_file, 'r', encoding='utf-8') as file:
            self.metadata = yaml.safe_load(file)       

        if not self.metadata or not self.raw_metadata:
            print("[Processor] Metadata is empty or not properly loaded.")
            return False

        if self.metadata.get('subreddit') != self.raw_metadata.get('subreddit') or \
           self.metadata.get('wiki_uri') != self.raw_metadata.get('wiki_uri') or \
           self.metadata.get('wiki_section') != self.raw_metadata.get('wiki_section'):
            print("[Processor] Metadata mismatch between processed and raw metadata.")
            return False
        
        markdown_files = glob(os.path.join(self.processed_dir, '*.md'))
        if not markdown_files:
            print("[Processor] No markdown files found in the processed directory.")
            return False
        
        markdown_set: set[str] = set(os.path.basename(f) for f in markdown_files)
        metadata_set: set[str] = set(chapter['filename'] for chapter in self.metadata.get('chapters', []))
        if markdown_set != metadata_set:
            print()
            print("[Processor] Mismatch between markdown files and metadata chapters.")
            return False
        
        return True
    
    def wipe(self) -> None:
        """
        Wipe the processed directory and remove all files.
        This method is used to reset the processor state in case of bad metadata.
        """
        for file in glob(os.path.join(self.processed_dir, '*.*')):
            os.remove(file)
        
        self.metadata = {
            'revision_date': self.raw_metadata.get('revision_date', ''),
            'subreddit': self.raw_metadata.get('subreddit', ''),
            'wiki_section': self.raw_metadata.get('wiki_section', ''),
            'wiki_uri': self.raw_metadata.get('wiki_uri', ''),
            'chapters': []
        }
        self.processing_queue = Queue()
        print(f"[Processor] Processed directory '{self.processed_dir}' has been wiped.")

    def fetch_all(self) -> None:
        """
        Fetch all chapters from the raw directory and enqueue them for processing.
        """
        for chapter in self.raw_metadata.get('chapters', []):
            chapter_path = os.path.join(self.raw_dir, chapter['filename'])
            self.processing_queue.put(chapter_path)
            print(f"[Processor] Enqueued chapter for processing: {chapter['filename']}")
        
        print(f"[Processor] Processing queue initialized with {self.processing_queue.qsize()} chapters.")

    def fetch_update(self) -> None:
        """
        Fetch new chapters from the raw directory, delete outdated chapters and enqueue them for processing.
        """

        chapters_df = pd.DataFrame(self.metadata.get('chapters', []))
        raw_chapters_df = pd.DataFrame(self.raw_metadata.get('chapters', []))

        merged_df = pd.merge(
            chapters_df, raw_chapters_df, on='url', how='right', suffixes=('_processed', '_raw')
        )

        for _, row in merged_df.iterrows():
            if pd.isna(row['revision_date_processed']):
                self.processing_queue.put(os.path.join(self.raw_dir, row['filename_raw']))
                print(f"[Processor] Enqueued new chapter for processing: {row['filename_raw']}")
            elif row['revision_date_raw'] > row['revision_date_processed']:
                self.processing_queue.put(os.path.join(self.raw_dir, row['filename_raw']))
                print(f"[Processor] Enqueued updated chapter for processing: {row['filename_raw']}")
                
                # Delete outdated chapter from processed directory and metadata
                outdated_file = os.path.join(self.processed_dir, row['filename_processed'])
                os.remove(outdated_file)
                print(f"[Processor] Deleted outdated chapter: {outdated_file}")
                chapters_df = chapters_df[chapters_df['filename'] != row['filename_processed']]
        
        self.metadata['chapters'] = chapters_df.to_dict(orient='records')

        if self.processing_queue.empty():
            print("[Processor] No new or updated chapters found to process.")
        else:
            print(f"[Processor] Processing queue updated with {self.processing_queue.qsize()} chapters.")
            with open(os.path.join(self.processed_dir, 'metadata.yaml'), 'w', encoding='utf-8') as file:
                yaml.safe_dump(self.metadata, file, allow_unicode=True)
            print("[Processor] Metadata removed outdated chapters.")

    def process_chapter(self, chapter_path: str) -> None:
        """
        Process a single chapter file. This method should be overridden by subclasses.
        """
        raise NotImplementedError("Subclasses must implement this method.")
    
    def run(self) -> None:
        """
        Run the processor. This method will fetch updates and process chapters.
        """

        if self.validate_metadata():
            self.fetch_update()
        else:
            print("[Processor] Metadata validation failed. Wiping processed directory.")
            self.wipe()
            self.fetch_all()

        raw_meta_df = pd.DataFrame(self.raw_metadata.get('chapters', []))

        while not self.processing_queue.empty():
            chapter_path = self.processing_queue.get()
            print(f"[Processor] Processing chapter: {chapter_path}")
            self.process_chapter(chapter_path)
            self.processing_queue.task_done()

            self.metadata['chapters'].append({
                'filename': os.path.basename(chapter_path),
                'url': raw_meta_df[raw_meta_df['filename'] == os.path.basename(chapter_path)]['url'].values[0],
                'revision_date': int(raw_meta_df[raw_meta_df['filename'] == os.path.basename(chapter_path)]['revision_date'].values[0])
            })
        
        print("[Processor] All chapters processed.")

        with open(os.path.join(self.processed_dir, 'metadata.yaml'), 'w', encoding='utf-8') as file:
            yaml.safe_dump(self.metadata, file, allow_unicode=True)
            print("[Processor] Metadata updated after processing chapters.")

