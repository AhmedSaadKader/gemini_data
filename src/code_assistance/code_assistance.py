from . import code_context
import os
from collections import defaultdict

def group_files_by_directory(files: list) -> dict:
    """Groups files by their directory for cleaner display"""
    grouped = defaultdict(list)
    for f in files:
        directory = os.path.dirname(f)
        filename = os.path.basename(f)
        grouped[directory].append(filename)
    return grouped

def provide_code_assistance(prompt):
    """
    Provides code assistance with complete codebase context and structure awareness
    
    Args:
        prompt: User's code-related question or request
        
    Returns:
        Formatted prompt with all code context
    """
    # Initialize the context manager
    context_manager = code_context.initialize_code_context()
    
    # Get all Python files
    python_files = context_manager.get_all_python_files()
    
    # Build context string
    code_context_str = context_manager.build_code_context_string()
    
    # Group files by directory for clearer structure display
    grouped_files = group_files_by_directory(python_files)
    structure_list = []
    for directory, files in sorted(grouped_files.items()):
        rel_dir = context_manager.get_relative_path(directory)
        structure_list.append(f"\n{rel_dir}/")
        for f in sorted(files):
            structure_list.append(f"  - {f}")
    
    project_structure = "\n".join(structure_list)
    
    final_prompt = f"""
    You are a software development expert. 

    Project Structure:
    {project_structure}
    
    Code Context:
    {code_context_str}
    
    User Question: {prompt}
    
    Please provide assistance based on the complete codebase context, taking into account the project's structure.
    """
    return final_prompt