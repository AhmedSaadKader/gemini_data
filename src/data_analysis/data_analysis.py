from src import gemini_api
from . import database
from . import data_processing

def analyze_data_and_generate_report(conn):
    """Analyzes the data from database and generate a report."""

    # 1. Execute SQL query
    query = "SELECT activeingredient, company, form, \"group\", new_price FROM drug_database LIMIT 100;"
    df = database.execute_query(conn, query)
    if df is None:
        return None

    # 2. Data Cleaning: Handle missing values
    df = data_processing.handle_missing_values(df)

    # 3. Format data as Markdown table
    markdown_table = data_processing.format_data_as_markdown_table(df)

    # 4. Craft prompt
    final_prompt = f"""
    You are an experienced data analyst examining a table of drug data from a PostgreSQL database.
    The data is presented in a Markdown table. You will not generate the table, it is just for you to reference
    The table is made of the following columns: activeingredient, company, form, group, new_price

    {markdown_table}

    Analyze the data and provide a concise summary, noting:

    *   The most frequent active ingredients.
    *   The main companies involved.
    *   The common drug forms.
    *   The different drug groups.
    *   The range of prices (minimum and maximum).
    *   The location of any rows that have empty strings in the cells.

    Provide a report suitable for management to review in order to make business decisions.
    """
    #Data Analysis do not require Code Context
    #Returning final prompt instead of prompt generation, as it does not make sense

    return final_prompt


def analyze_active_ingredients(conn):
    """
    Analyzes active ingredients for inconsistencies and generates a prompt for AI 
    to clean and standardize the data.
    
    Args:
        conn: Database connection object
    
    Returns:
        str: Prompt for AI analysis and cleaning of active ingredients
    """
    # Execute SQL query to get only active ingredients
    query = "SELECT DISTINCT activeingredient FROM drug_database WHERE activeingredient IS NOT NULL;"
    df = database.execute_query(conn, query)
    if df is None:
        return None
        
    # Handle missing values
    df = data_processing.handle_missing_values(df)
    
    # Format data as Markdown table
    markdown_table = data_processing.format_data_as_markdown_table(df)
    
    # Craft prompt for AI analysis
    final_prompt = f"""
    You are a pharmaceutical data specialist analyzing a list of active ingredients from a drug database.
    Below is a markdown table containing all unique active ingredients:

    {markdown_table}

    Please analyze this data and:

    1. Identify potential duplicates due to:
       - Spelling variations or typos
       - Different naming conventions (e.g., "Acetaminophen" vs "APAP")
       - Case differences
       - Extra spaces or special characters
       
    2. For each group of potential duplicates:
       - List all variations found
       - Recommend the standardized name to use
       - Explain why this standardization is recommended
       
    3. Provide the whole list of active ingredients in the following CSV format:
       index,active_ingredient
       
    Rules for standardization:
    - Use proper capitalization (capitalize first letter of each word)
    - Remove unnecessary spaces
    - Use most commonly accepted international naming convention
    - Be consistent with abbreviations
    - Remove any special characters unless chemically significant
    
    Format your response as follows:
    1. Analysis summary
    2. List of identified issues
    3. Standardization recommendations
    4. CSV data (starting with "CSV_START" and ending with "CSV_END")
    """
    
    return final_prompt

def analyze_and_clean_active_ingredients(conn, chunk_size=100, offset=0):
    """
    Analyzes active ingredients in chunks and generates a prompt for AI to create a cleaned,
    standardized CSV with individual ingredients.
    
    Args:
        conn: Database connection object
        chunk_size: Number of records to process at a time
        offset: Starting point for pagination
    
    Returns:
        tuple: (prompt, has_more_data, next_offset)
            - prompt: str, The prompt for AI analysis
            - has_more_data: bool, Whether there are more records to process
            - next_offset: int, The next offset to use for pagination
    """
    # Execute SQL query with pagination
    query = """
        SELECT DISTINCT activeingredient 
        FROM drug_database 
        WHERE activeingredient IS NOT NULL 
        ORDER BY activeingredient
        LIMIT %s OFFSET %s;
    """
    df = database.execute_query(conn, query, params=(chunk_size, offset))
    if df is None:
        return None, False, offset
        
    # Get total count for progress tracking
    count_query = "SELECT COUNT(DISTINCT activeingredient) FROM drug_database WHERE activeingredient IS NOT NULL;"
    total_df = database.execute_query(conn, count_query)
    total_count = total_df.iloc[0, 0] if total_df is not None else 0
    
    # Check if there are more records
    has_more_data = (offset + chunk_size) < total_count
    next_offset = offset + chunk_size if has_more_data else offset
    
    # Handle missing values
    df = data_processing.handle_missing_values(df)
    
    # Format data as Markdown table
    markdown_table = data_processing.format_data_as_markdown_table(df)
    
    # Craft prompt for AI analysis
    final_prompt = f"""
    You are a pharmaceutical data specialist analyzing a list of active ingredients from a drug database.
    This is chunk {offset//chunk_size + 1} of {(total_count + chunk_size - 1)//chunk_size} total chunks.
    Below is a markdown table containing active ingredients for this chunk:

    {markdown_table}

    Please process this chunk of data and provide:

    1. A brief analysis summary for this chunk including:
       - Number of ingredients processed in this chunk
       - Number of combination products identified in this chunk
       
    2. A clean CSV list of all individual active ingredients where:
       - Each row contains exactly one ingredient
       - Combination products are split into separate rows
       - Names are standardized using proper capitalization
       - Unnecessary spaces and special characters are removed
       - Most commonly accepted international naming conventions are used
       
    Format your response as follows:
    1. Analysis Summary (2-3 sentences)
    2. CSV data (starting with "CSV_START" and ending with "CSV_END")
       Format: index,active_ingredient
       Note: Continue the index from {offset + 1}
       
    Only include these two sections in your response.
    """
    
    return final_prompt, has_more_data, next_offset

def process_all_ingredients(conn):
    """
    Process all active ingredients in chunks and combine the results.
    
    Args:
        conn: Database connection object
        
    Returns:
        list: List of generated prompts and their responses
    """
    chunk_size = 100
    offset = 0
    results = []
    
    while True:
        prompt, has_more_data, next_offset = analyze_and_clean_active_ingredients(
            conn, 
            chunk_size=chunk_size, 
            offset=offset
        )
        
        if prompt is None:
            break
            
        # Generate content using the Gemini API
        analysis_result, _ = gemini_api.generate_content(prompt)
        
        if analysis_result:
            results.append(analysis_result)
        
        if not has_more_data:
            break
            
        offset = next_offset
        
    return results

def save_combined_results(results, file_handler):
    """
    Combine and save all results from the chunked processing.
    
    Args:
        results: List of analysis results
        file_handler: FileHandler instance for saving output
    """
    combined_analysis = "Combined Analysis Summary:\n\n"
    combined_csv = "index,active_ingredient\n"
    
    for result in results:
        # Split the result into analysis and CSV parts
        parts = result.split("CSV_START")
        if len(parts) > 1:
            analysis = parts[0].strip()
            csv_data = parts[1].split("CSV_END")[0].strip()
            
            # Add to combined results
            combined_analysis += analysis + "\n\n"
            combined_csv += csv_data + "\n"
    
    # Save combined results
    final_output = f"{combined_analysis}\nCSV_START\n{combined_csv}\nCSV_END"
    return file_handler.save_output(final_output)
    """
    Analyzes active ingredients and generates a prompt for AI to create a cleaned,
    standardized CSV with individual ingredients.
    
    Args:
        conn: Database connection object
    
    Returns:
        str: Prompt for AI to generate cleaned ingredient CSV
    """
    # Execute SQL query to get only active ingredients
    query = "SELECT DISTINCT activeingredient FROM drug_database WHERE activeingredient IS NOT NULL;"
    df = database.execute_query(conn, query)
    if df is None:
        return None
        
    # Handle missing values
    df = data_processing.handle_missing_values(df)
    
    # Format data as Markdown table
    markdown_table = data_processing.format_data_as_markdown_table(df)
    
    # Craft prompt for AI analysis
    final_prompt = f"""
    You are a pharmaceutical data specialist analyzing a list of active ingredients from a drug database.
    Below is a markdown table containing all unique active ingredients:

    {markdown_table}

    Please process this data and provide:

    1. A brief analysis summary including:
       - Total number of unique ingredients found
       - Number of combination products identified
       - Any notable patterns in the data
       
    2. A clean CSV list of all individual active ingredients where:
       - Each row contains exactly one ingredient
       - Combination products are split into separate rows
       - Names are standardized using proper capitalization
       - Unnecessary spaces and special characters are removed
       - Most commonly accepted international naming conventions are used
       
    Format your response as follows:
    1. Analysis Summary (2-3 sentences)
    2. CSV data (starting with "CSV_START" and ending with "CSV_END")
       Format: index,active_ingredient
       
    Only include these two sections in your response.
    """
    
    return final_prompt