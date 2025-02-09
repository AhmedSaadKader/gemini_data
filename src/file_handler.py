import os
from datetime import datetime
import re

class FileHandler:
    def __init__(self, base_output_dir="outputs"):
        """Initialize FileHandler with base output directory."""
        self.base_output_dir = base_output_dir
        self._ensure_output_directories()

    def _ensure_output_directories(self):
        """Create output directories if they don't exist."""
        # Create main output directory
        os.makedirs(self.base_output_dir, exist_ok=True)
        
        # Create subdirectories for different types of outputs
        self.analysis_dir = os.path.join(self.base_output_dir, "analysis_reports")
        self.code_assist_dir = os.path.join(self.base_output_dir, "code_assistance")
        os.makedirs(self.analysis_dir, exist_ok=True)
        os.makedirs(self.code_assist_dir, exist_ok=True)

    def _generate_filename_from_content(self, content, max_words=5):
        """Generate a filename from the content's first line or significant keywords."""
        # Extract first line or significant text
        first_line = content.strip().split('\n')[0]
        
        # Remove special characters and convert to lowercase
        clean_text = re.sub(r'[^\w\s-]', '', first_line.lower())
        
        # Get first few words
        words = clean_text.split()[:max_words]
        
        # Join words with hyphens
        return '-'.join(words)

    def _get_unique_filepath(self, directory, base_name, extension):
        """Generate a unique filepath with date and counter if needed."""
        date_str = datetime.now().strftime("%Y%m%d")
        
        # Start with counter = 1
        counter = 1
        while True:
            # Format: YYYYMMDD_counter_basename.extension
            filename = f"{date_str}_{counter:02d}_{base_name}.{extension}"
            filepath = os.path.join(directory, filename)
            
            if not os.path.exists(filepath):
                return filepath
            counter += 1

    def save_output(self, content, file_type="analysis", code_prompt=None):
        """
        Save output content to an appropriately named file in the correct directory.
        
        Args:
            content (str): The content to save
            file_type (str): Type of output ("analysis" or "code_assistance")
            code_prompt (str, optional): The original code prompt if applicable
        
        Returns:
            str: Path to the saved file
        """
        # Determine output directory
        output_dir = self.analysis_dir if file_type == "analysis" else self.code_assist_dir
        
        # Generate base name from content
        if code_prompt:
            base_name = self._generate_filename_from_content(code_prompt)
        else:
            base_name = self._generate_filename_from_content(content)
        
        # Get unique filepath
        filepath = self._get_unique_filepath(output_dir, base_name, "md")
        
        # Write content to file
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                if code_prompt:
                    f.write(f"# Code Assistance Request: {code_prompt}\n\n")
                f.write(content)
            
            return filepath
        except IOError as e:
            raise IOError(f"Error saving file {filepath}: {e}")