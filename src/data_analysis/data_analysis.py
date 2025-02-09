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