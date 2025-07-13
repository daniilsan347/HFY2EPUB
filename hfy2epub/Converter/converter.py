import subprocess as sp
import os
import yaml
from glob import glob
import re
import tempfile
from datetime import datetime

class Converter:
    def __init__(self, wiki_dir: str, processed_dir: str) -> None:
        """
        Initialize the Converter with directories and title.

        :param wiki_dir: Directory containing wiki files.
        :param processed_dir: Directory for processed files.
        :param title: Title for the EPUB file.
        """
        self.wiki_dir = wiki_dir
        self.processed_dir = processed_dir
        self.output_dir = os.path.join(os.getcwd(), 'output')
        os.makedirs(self.output_dir, exist_ok=True)

    def make_metadata(self) -> None:
        """
        Create metadata for the EPUB file.

        :return: Dictionary containing metadata.
        """

        first_date, last_date = self.get_dates()
        cover_image = os.path.join(self.wiki_dir, 'cover.jpg').replace('\\', '/')

        wiki_files = glob(os.path.join(self.wiki_dir, '*.yaml'))
        if not wiki_files:
            raise ValueError("No wiki files found in the wiki directory.")
        
        latest_wiki_file = max(wiki_files, key=os.path.getmtime)

        with open(latest_wiki_file, 'r', encoding='utf-8') as f:
            wiki_data: dict = yaml.safe_load(f)
        if not wiki_data:
            raise ValueError("Wiki data not found. Ensure 'wiki.yaml' exists in the wiki directory.")
        
        author = wiki_data.get('author', 'Unknown Author')
        title = wiki_data.get('wiki_section', 'Unknown Title')

        self.metadata = {
            'date': first_date,
            'lang': 'en-US',
            'revision_date': last_date,
            'title': title,
            'publisher': 'r/HFY',
            'creator': [
                {
                    'role': 'author',
                    'text': f'{author}'
                }
            ]
        }

        if os.path.exists(cover_image):
            self.metadata['cover-image'] = cover_image
    
    def get_dates(self) -> tuple[str, str]:
        """
        Get the first and last chapter dates from the processed file names."""

        processed_files = glob(os.path.join(self.processed_dir, '*.md'))
        if not processed_files:
            raise ValueError("No processed files found in the directory.")
        
        pattern = re.compile(r'(\d+).+\.md$')
        unix_dates = [match.group(1) for f in processed_files if (match := pattern.search(os.path.basename(f)))]
        if not unix_dates:
            raise ValueError("No valid dates found in processed file names.")
        first_date = min(unix_dates)
        last_date = max(unix_dates)
        first_date = datetime.fromtimestamp(int(first_date)).strftime('%Y-%m-%d')
        last_date = datetime.fromtimestamp(int(last_date)).strftime('%Y-%m-%d')

        return first_date, last_date

    def dump_temp_file(self) -> str:
        """
        Dump metadata and all chapters to a temporary file.

        :return: Path to the temporary file.
        """
        processed_files = glob(os.path.join(self.processed_dir, '*.md'))
        with tempfile.NamedTemporaryFile(delete=False, suffix='.md') as temp_file:
            temp_file.write('---\n'.encode('utf-8'))
            temp_file.write(yaml.dump(self.metadata, allow_unicode=True, default_flow_style=False).encode('utf-8'))
            temp_file.write('---\n\n'.encode('utf-8'))

            for file in processed_files:
                with open(file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    temp_file.write(content.encode('utf-8'))
                    temp_file.write('\n\n'.encode('utf-8'))
            self.temp_file_path = temp_file.name
        return self.temp_file_path
    
    def execute_pandoc(self) -> None:
        """
        Execute Pandoc to convert the temporary file to EPUB format.
        """
        if not hasattr(self, 'temp_file_path'):
            raise ValueError("Temporary file path not set. Call dump_temp_file() first.")
        
        output_file = os.path.join(self.output_dir, f'{self.metadata['title']} - {self.metadata['creator'][0]['text']}.epub')
        command = [
            'pandoc',
            self.temp_file_path,
            '-o', output_file
        ]
        
        try:
            sp.run(command, check=True, stdout=sp.PIPE, stderr=sp.PIPE, text=True)
            print(f"[Converter] EPUB file created successfully at {output_file}")
        except sp.CalledProcessError as e:
            print(f"[Converter] Error during conversion: {e}")
            print(f"[Converter] stdout: {e.stdout}")
            print(f"[Converter] stderr: {e.stderr}")
        finally:
            os.remove(self.temp_file_path)

    def run(self) -> None:
        """
        Run the conversion process.
        """
        self.make_metadata()
        temp_file_path = self.dump_temp_file()
        print(f"[Converter] Temporary file created at {temp_file_path}")
        self.execute_pandoc()
