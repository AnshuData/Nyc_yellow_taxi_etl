import logging as log
from src.config import load_config

from src.setup_pipeline import run as setup_pipeline_run
from src.extract_data import ingest_nyc_tlc_yellow_data
from src.clean_data import run as data_clean_run
from src.data_quality_check import run as data_quality_check_run
from src.data_summarize import run as nyc_taxi_statistics_run


def run():

    # Setup logging, data and log directories; creates if does not exist
    setup_pipeline_run()

    # load config file containing config parameters such as input, output file path , start and end date
    cfg = load_config("config/config.yaml")

    # ingest and save ingested data
    log.info("Data ingestion started")
    ingest_nyc_tlc_yellow_data(cfg["start_date"], cfg["end_date"], cfg["landing_zone_file"])
    log.info("Data ingestion ended")

    # cleanse data
    data_clean_run(cfg["landing_zone_file"], cfg["bronze_layer_file"])
    
    # check data quality
    log.info("Data quality check started")
    data_quality_check_run(cfg["bronze_layer_file"], cfg["silver_layer_file"])
    log.info("Data quality check ended")

    # transformations
    log.info("Data transformation check started")
    nyc_taxi_statistics_run(cfg["silver_layer_file"], cfg["gold_layer_file"])
    log.info("Data transformation ended")


def main() -> None:
    run()


if __name__ == "__main__":
    main()
