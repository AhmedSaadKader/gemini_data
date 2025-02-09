from . import gemini_api
from .file_handler import FileHandler

# Initialize the file handler
file_handler = FileHandler()

def generate_analysis_file(prompt, output_file=None, code_prompt=None):
    """Processes the prompt using the gemini API and writes the output to a file.
    
    Args:
        prompt (str): The prompt to send to Gemini
        output_file (str, optional): Deprecated parameter kept for backwards compatibility
        code_prompt (str, optional): The original code prompt if this is a code assistance request
    """
    analysis_result, usage_metadata = gemini_api.generate_content(prompt)

    if analysis_result:
        try:
            # Determine the type of output
            file_type = "code_assistance" if code_prompt else "analysis"
            
            # Save the file using the file handler
            saved_filepath = file_handler.save_output(
                content=analysis_result,
                file_type=file_type,
                code_prompt=code_prompt
            )

            print(f"\nAnalysis saved to: {saved_filepath}")

            # Print token usage information if available
            if usage_metadata:
                print("\nToken Usage:")
                print(f"  Prompt Tokens: {usage_metadata.prompt_token_count}")
                print(f"  Candidate Tokens: {usage_metadata.candidates_token_count}")
                print(f"  Total Tokens: {usage_metadata.total_token_count}")
            else:
                print("\nToken Usage: Not available.")
        except IOError as e:
            print(f"Error writing to file: {e}")
    else:
        print("Gemini API request failed.")

def count_tokens(prompt):
    """Counts the number of tokens in a given text prompt."""
    model = gemini_api.model
    return model.count_tokens(prompt)