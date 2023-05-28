from bikeshareai.bluebikes import BlueBikesDataPipeline
# Create an instance of BlueBikesDataPipeline and run the methods
pipeline = BlueBikesDataPipeline('https://s3.amazonaws.com/hubway-data/202304-bluebikes-tripdata.zip')
pipeline.unzip_file_to_local_csv()
pipeline.csv_to_db('journeys', 'data/202304-bluebikes-tripdata.csv')
pipeline.geojoin('data/Boston_Neighborhoods.geojson')
pipeline.enrich_journeys()

