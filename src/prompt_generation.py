def create_gemini_prompt(markdown_table, code_context):
    """Creates the full Gemini prompt."""
    prompt = f"""
    You are an experienced data analyst examining a table of drug data from a PostgreSQL database.
    The data is presented in a Markdown table. You will not generate the table, it is just for you to reference
    The table is made of the following columns: activeingredient, company, form, group, new_price

    {markdown_table}

    Here is some potentially relevant code from the application:

    {code_context}

    Analyze the data and provide a concise summary, noting:

    *   The most frequent active ingredients.
    *   The main companies involved.
    *   The common drug forms.
    *   The different drug groups.
    *   The range of prices (minimum and maximum).
    *   The location of any rows that have empty strings in the cells.

    Provide a report suitable for management to review in order to make business decisions.
    """
    return prompt