import os
from typing import List, Dict, Set
from pathlib import Path

class CodeContextManager:
    """Manages code context with awareness of project structure"""
    
    def __init__(self):
        self.known_files = {
            'src/': [
                'main.py',
                'gemini_api.py',
                'output.py',
                'config.py',
                'prompt_generation'
            ],
            'src/data_analysis/': [
                'data_analysis.py',
                'data_processing.py',
                'database.py',
            ],
            'src/code_assistance/': [
                'code_assistance.py',
                'code_context.py',
            ]
        }
        # Base directory is where code_context.py is located
        self.base_dir = os.path.dirname(__file__)
        # Project root is two levels up from code_context.py
        self.project_root = os.path.dirname(os.path.dirname(self.base_dir))

    def get_full_path(self, relative_path: str) -> str:
        """
        Converts a relative path to full path based on project structure
        
        Args:
            relative_path: Path relative to project root
            
        Returns:
            Full path from current directory
        """
        return os.path.join(self.project_root, relative_path)

    def get_relative_path(self, full_path: str) -> str:
        """
        Gets the path relative to project root
        
        Args:
            full_path: Absolute path of file
            
        Returns:
            Path relative to project root
        """
        try:
            return os.path.relpath(full_path, self.project_root)
        except ValueError:
            return full_path

    def get_all_python_files(self) -> List[str]:
        """
        Gets all known Python files with their proper paths
        
        Returns:
            List of full paths to Python files
        """
        python_files = []
        for directory, files in self.known_files.items():
            for file in files:
                if file.endswith('.py'):
                    relative_path = os.path.join(directory, file)
                    full_path = self.get_full_path(relative_path)
                    python_files.append(full_path)
        return python_files

    def read_file(self, filepath: str) -> str:
        """
        Reads the content of a file with improved error handling
        
        Args:
            filepath: Path to the file to read
            
        Returns:
            String containing the file content or error message
        """
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
                return content
        except FileNotFoundError:
            relative_path = self.get_relative_path(filepath)
            return f"Error: File not found at {relative_path}"
        except Exception as e:
            relative_path = self.get_relative_path(filepath)
            return f"Error reading file {relative_path}: {e}"

    def build_code_context_string(self) -> str:
        """
        Builds a formatted context string from all Python files
        
        Returns:
            Formatted string containing the context from all files
        """
        code_context = []
        python_files = self.get_all_python_files()
        
        # Sort files by directory to group related files together
        python_files.sort()
        
        for filepath in python_files:
            relative_path = self.get_relative_path(filepath)
            content = self.read_file(filepath)
            
            code_context.extend([
                f"\n{'='*20} File: {relative_path} {'='*20}",
                content,
                f"{'='*20} End of {relative_path} {'='*20}\n"
            ])
        
        return "\n".join(code_context)

# Function to initialize the context manager
def initialize_code_context() -> CodeContextManager:
    """Creates and returns a new CodeContextManager instance"""
    return CodeContextManager()

# Updated functions to maintain backward compatibility
def get_relevant_code(task_description: str) -> List[str]:
    """
    Returns all Python files regardless of task description
    """
    manager = initialize_code_context()
    return manager.get_all_python_files()

def build_code_context_string(relevant_code_files: List[str]) -> str:
    """
    Builds context string from all Python files
    """
    manager = initialize_code_context()
    return manager.build_code_context_string()