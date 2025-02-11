from .file_handler import FileHandler
from .data_analysis import database
from .gemini_api import initialize_gemini
from .data_analysis import data_analysis
from .output import generate_analysis_file

def main():
    # Initialize components
    initialize_gemini()
    file_handler = FileHandler()
    
    # Connect to database
    conn = database.connect_to_db()
    if not conn:
        exit()
    
    try:
        # Process all ingredients
        results = data_analysis.process_all_ingredients(conn)
        
        # Save combined results
        output_path = data_analysis.save_combined_results(results, file_handler)
        print(f"Saved combined results to: {output_path}")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()