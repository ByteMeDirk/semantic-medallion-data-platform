# A local script to run the entire ETL pipeline

# Bronze Layer
#python semantic_medallion_data_platform/bronze/brz_01_extract_known_entities.py --raw_data_filepath "./data/known_entities/*.csv"
#python semantic_medallion_data_platform/bronze/brz_01_extract_newsapi.py --days_back "1"

# Silver Layer
python semantic_medallion_data_platform/silver/slv_02_transform_nlp_newsapi.py
python semantic_medallion_data_platform/silver/slv_02_transform_nlp_known_entities.py
python semantic_medallion_data_platform/silver/slv_03_transform_entity_to_entity_mapping.py
python semantic_medallion_data_platform/silver/slv_02_transform_sentiment_newsapi.py

# Gold Layer
python semantic_medallion_data_platform/gold/gld_04_load_entities.py
python semantic_medallion_data_platform/gold/gld_04_load_newsapi.py
python semantic_medallion_data_platform/gold/gld_04_load_newsapi_sentiment.py
