from prefect.artifacts import create_markdown_artifact
import pandas as pd
from datetime import datetime

def ingest_report(tableNewTransients, tableUpdatedTransients, tableDeactivatedTransients):
    """
    Generates a markdown report for the ingested/updated transients.
    
    Args:
        tableNewTransients (pd.DataFrame): The DataFrame containing the new transients.
        tableUpdatedTransients (pd.DataFrame): The DataFrame containing the updated transients.
        tableDeactivatedTransients (pd.DataFrame): The DataFrame containing the deactivated transients.
    """
    
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    

    markdown_report = f"# TiDES Ingest Report\n"
    markdown_report += f"**Date:** {date_str}\n\n"

    def generate_section(title, data, columns=None):
        # Check if data is a list (and empty) or None
        if isinstance(data, list):
            if not data:
                return ""
            # If it's a non-empty list, we assume it might be a list of dicts that can be converted
            # But the user code implies these should be DataFrames. 
            # If it's a list, let's try to convert it just in case, or ignore if truly just []
            try:
                df = pd.DataFrame(data)
            except:
                return ""
        else:
            df = data

        # Check if it's a DataFrame and not empty
        if isinstance(df, pd.DataFrame) and not df.empty:
            section = f"## {title}\n"
            
            # Select columns if provided and they exist
            if columns:
                existing_cols = [c for c in columns if c in df.columns]
                if existing_cols:
                    df = df[existing_cols]
            
            try:
                section += df.to_markdown(index=False, tablefmt="pipe")
            except ImportError:
                section += df.to_csv(sep="|", index=False)
            
            return section + "\n\n"
        
        return ""

    markdown_report += generate_section("New Transients", tableNewTransients, ['tides_id','pk_4most','name','ra','dec'])
    markdown_report += generate_section("Updated Transients", tableUpdatedTransients)
    markdown_report += generate_section("Deactivated Transients", tableDeactivatedTransients)

    # If report is empty (besides header), mention it
    if len(markdown_report.split('\n')) <= 4:
         markdown_report += "_No changes to report._"

    # Create the artifact
    artifact_key = f"tides-ingest-report"

    create_markdown_artifact(
        key=artifact_key,
        markdown=markdown_report,
        description=f"TiDES Ingest Report - {date_str}"
    )

    return markdown_report
