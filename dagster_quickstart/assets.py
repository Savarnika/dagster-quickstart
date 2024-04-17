import json
import requests

import pandas as pd

from dagster import (
    MaterializeResult,
    MetadataValue,
    asset,
    get_dagster_logger,
)
from dagster_quickstart.configurations import HNStoriesConfig, NaptanConfig


@asset
def hackernews_top_story_ids(config: HNStoriesConfig):
    """Get top stories from the HackerNews top stories endpoint."""
    top_story_ids = requests.get("https://hacker-news.firebaseio.com/v0/topstories.json").json()

    with open(config.hn_top_story_ids_path, "w") as f:
        json.dump(top_story_ids[: config.top_stories_limit], f)


@asset(deps=[hackernews_top_story_ids])
def hackernews_top_stories(config: HNStoriesConfig) -> MaterializeResult:
    """Get items based on story ids from the HackerNews items endpoint."""
    with open(config.hn_top_story_ids_path, "r") as f:
        hackernews_top_story_ids = json.load(f)

    results = []
    for item_id in hackernews_top_story_ids:
        item = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json").json()
        results.append(item)

    df = pd.DataFrame(results)
    df.to_csv(config.hn_top_stories_path)

    return MaterializeResult(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(str(df[["title", "by", "url"]].to_markdown())),
        }
    )


def add_geojson(row):
    """
    Return GeoJSON from Latitude and Longitude coordinates for each row of Dataframe
    """
    return {
        "type": "Point",
        "coordinates": [row['longitude'], row['latitude']]
    }


def pascal_to_snake(s):
    """
    Convert PascalCase to snake_case.
    """
    # Check each letter in the string; Convert to lower case if the character is
    # not the first letter, is in upper case and if its adjacent character is in lower case
    snake_case = ''.join(['_' + c.lower() if c.isupper() and s[i+1].islower() and i!=0 else c.lower() for i, c in enumerate(s)])
    return snake_case


@asset
def naptan_stops_data(config: NaptanConfig):
    """Get stops from the Naptan access nodes endpoint and add geom column."""

    # Read data from Naptan API as a Pandas Dataframe
    naptan_stops_df = pd.read_csv("https://naptan.api.dft.gov.uk/v1/access-nodes?dataFormat=csv")
    # Publish the DataFrame as a CSV file in a tmp/ folder
    naptan_stops_df.to_csv(config.naptan_raw_data_path)

    # Convert the column names from Pascal Case to Snake Case to maintain a database standard
    naptan_stops_df.columns = naptan_stops_df.columns.map(pascal_to_snake)
    # Add geom column by using longitude and latitude columns
    naptan_stops_df['geom'] = naptan_stops_df.apply(add_geojson, axis=1)
    # Publish the cleaned DataFrame to a CSV file in a tmp/folder
    naptan_stops_df.to_csv(config.naptan_clean_data_path, index=False)

    return MaterializeResult(
        metadata={
            "num_records": len(naptan_stops_df),
            "naptan_data_path": MetadataValue.md(str(config.naptan_clean_data_path)),
        }
    )