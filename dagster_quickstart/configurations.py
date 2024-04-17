from dagster import Config


class HNStoriesConfig(Config):
    top_stories_limit: int = 10
    hn_top_story_ids_path: str = "hackernews_top_story_ids.json"
    hn_top_stories_path: str = "hackernews_top_stories.csv"


class NaptanConfig(Config):
    naptan_raw_data_path: str = "tmp/naptan_raw_data.csv"
    naptan_clean_data_path: str = "tmp/naptan_clean_data.csv"
