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

    def dataframe_to_markdown(df):
        if not df is None:
            try:
                return df.to_markdown(index=False, tablefmt="pipe")
            except ImportError:
                return df.to_csv(sep="|", index=False)
        return "_No objects in this category._"

    markdown_report += "## New Transients\n"
    markdown_report += dataframe_to_markdown(tableNewTransients[['tides_id','pk_4most','name','ra','dec']]) + "\n\n"

    markdown_report += "## Updated Transients\n"
    markdown_report += dataframe_to_markdown(tableUpdatedTransients) + "\n\n"

    markdown_report += "## Deactivated Transients\n"
    markdown_report += dataframe_to_markdown(tableDeactivatedTransients) + "\n\n"

    # Create the artifact
    artifact_key = f"tides-ingest-report"

    create_markdown_artifact(
        key=artifact_key,
        markdown=markdown_report,
        description=f"TiDES Ingest Report - {date_str}"
    )

    return markdown_report
