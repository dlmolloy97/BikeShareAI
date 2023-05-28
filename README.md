
# Blue Bikes Data Pipeline

This repository contains a data pipeline for the Blue Bikes bike sharing system in Boston. The pipeline downloads the data from the Blue Bikes website, joins it to a shapefile of Boston neighbourhoods, and then loads it into a PostgreSQL database. It also contains a dashboard that allows the user to visualise the data.

## Installation

To install the package, run the following command in the terminal:

```
pip install git+
```

## Usage

To use the package, run the following commands in the terminal:

```
from bikeshareai.bluebikes import BlueBikesDataPipeline, DashboardBike
# Create an instance of BlueBikesDataPipeline and run the methods
pipeline = BlueBikesDataPipeline('https://s3.amazonaws.com/hubway-data/202304-bluebikes-tripdata.zip')
pipeline.unzip_file_to_local_csv()
pipeline.main_join('data/Boston_Neighborhoods.geojson', 'data/current_bluebikes_stations.csv', 'data/202304-bluebikes-tripdata.csv')
pipeline.enrich_journeys()
# Create an instance of DashboardBike and run the run method
dashboard = DashboardBike()
dashboard.run()
```

## License

This package is licensed under the MIT License. See the LICENSE file for details.

## Credits

This package was created by Desmond Molloy.

