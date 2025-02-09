# src/main.py
from .data_analysis import database
from . import gemini_api
from .data_analysis import data_analysis
from .code_assistance import code_assistance
from . import output
def main():
    # 1. Initialize Gemini API
    gemini_api.initialize_gemini()

    mode = input("What mode do you want to use? Data Analysis or Code Assistance (d/c): ").lower()

    if mode == 'd':
        # 2. Connect to the database
        conn = database.connect_to_db()
        if not conn:
            exit()
        prompt = data_analysis.analyze_data_and_generate_report(conn)
        output.generate_analysis_file(prompt, output_file="analysis_report.md")  # Changed extension to .md
        conn.close()

    elif mode == 'c':
        code_prompt = input("Please provide code prompt: ")
        prompt = code_assistance.provide_code_assistance(code_prompt)
        # Print confirmation of files being analyzed
        print("\nGenerating response based on codebase context...")
        output.generate_analysis_file(prompt, output_file="code_assistance.md", code_prompt=code_prompt)  # Changed extension to .md

    else:
        print("Invalid mode selected")

if __name__ == "__main__":
    main()