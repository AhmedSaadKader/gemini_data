# src/gemini_api.py
import google.generativeai as genai
from . import config

model = None  # Define global model

def initialize_gemini():
    """Initializes the Gemini API."""
    global model  # Make model accessible
    genai.configure(api_key=config.GOOGLE_API_KEY)
    model = genai.GenerativeModel('gemini-2.0-flash')  # Access the Gemini API here

def generate_content(prompt):
    """Generates content using the Gemini API.
    
    Args:
        prompt (str): The prompt to send to Gemini
        
    Returns:
        tuple: (generated_text, usage_metadata)
            - generated_text (str): The generated response text
            - usage_metadata (object): Metadata about token usage
    """
    if model is None:
        raise RuntimeError("Gemini API not initialized. Call initialize_gemini() first.")
    
    try:
        # Generate the response
        response = model.generate_content(prompt)
        
        # Extract the text from the response
        generated_text = response.text
        
        # Get usage metadata if available
        usage_metadata = getattr(response, 'usage_metadata', None)
        
        return generated_text, usage_metadata
    
    except Exception as e:
        print(f"Error generating content: {e}")
        return None, None