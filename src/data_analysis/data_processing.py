import pandas as pd

def handle_missing_values(df):
    """Handles missing values in the DataFrame by replacing them with empty strings."""
    return df.fillna('')

def format_data_as_markdown_table(df):
    """Formats a Pandas DataFrame as a Markdown table."""
    return df.to_markdown(index=False)