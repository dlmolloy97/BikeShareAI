# Create two classes:
# The first one should include the following methods:
# __init__: Takes a URL of a zip file as argument, and unzips the file into a CSV in the data folder
# connect_to_db: Creates and returns a connection to the local PostgreSQL database using the sqlalchemy package.
# csv_to_db: Takes the arguments table_name and csv_path. If no table with the name table_name exists in the local PostgreSQL, then insert the csv located at csv_path as a table with the name table_name
# geojoin: Takes the argument geojson_path. Runs the csv_to_db method on 'data/current_bluebikes_stations.csv', with stations as the table_name. Loads the data from geojson path as polydf, loads the table stations as a Geopandas GeoDataFrame with epsg='4326', and then performs a spatial join between polydf and the stations GeoDataFrame, using pandas to select the stations and neighbourhood for stations in Boston. The resulting Dataframe should be written to the local PostgreSQL database as the table neighbourhood_stations
# enrich_journeys: This method joins the journeys and neighbourhood_stations tables using journeys.start_station_name = neighbourhood_stations.station twice, producing a table with all of the columns from journeys, plus the start and end neighbourhoods and stations for each journey, creating a table in the PostgreSQL database called journeys_enriched
# The second one should include the following methods:
# - create_dash_application: create a Dash app that displays the number of trips by start and end neighbourhood, from the journeys_enriched table
# - run: call the create_dash_application method and run the app
from sqlalchemy import create_engine
import pandas as pd
from urllib import request
from zipfile import ZipFile
from geopandas import GeoDataFrame, read_file, points_from_xy
import dash
#import dash_core_components as dcc
from dash import dcc
#import dash_html_components as html
from dash import html
import plotly.express as px

class BlueBikesDataPipeline:

    def __init__(self, url):
        self.url = url
        self.engine = create_engine('postgresql://postgres:postgres@localhost:5432/desmondmolloy')
        # Create a connection to the engine called `conn`
        self.conn = self.engine.connect()
    
    def unzip_file_to_local_csv(self):
        # Download the zip file from the URL
        request.urlretrieve(self.url, 'data.zip')
        # Unzip the file
        ZipFile('data.zip').extractall('data')
        # Return the unzipped file
        # return 'data/tripdata.csv'
    
    def csv_to_db(self, table_name, csv_path):
        # Read in the DataFrame from the CSV file
        df = pd.read_csv(csv_path)
        # Append the data to the `trips` table
        df.to_sql(table_name, self.conn, index=False, if_exists='append')
    
    def geojoin(self, geojson_path):
        # Read in the DataFrame from the CSV file
        df = pd.read_csv('data/current_bluebikes_stations.csv')
        # Append the data to the `trips` table
        df.to_sql('stations', self.conn, index=False, if_exists='append')
        # Boston neighbourhoods
        polydf = read_file(geojson_path)
        stations = pd.read_sql('SELECT * FROM stations', self.conn)
        pointdf = GeoDataFrame(
            stations, geometry=points_from_xy(stations.Longitude, stations.Latitude))
        pointdf.set_crs(epsg='4326', inplace=True)
        joined_df = pointdf.sjoin(polydf, how="left")
        grab_df = joined_df[['Name_left', 'Name_right', 'District']]
        matched_pairs_with_pandas = grab_df[grab_df['District'] == 'Boston']
        matched_pairs_with_pandas.columns = ['station', 'neighbourhood', 'District']
        #matched_pairs = sqldf.run('SELECT DISTINCT Name_left as station, Name_right as neighbourhood from grab_df where District = \'Boston\'')
        matched_pairs_with_pandas.to_sql('neighbourhood_stations', self.conn, index=False, if_exists='append')

    def enrich_journeys(self):
        # Create the SQL query
        sql_query = """
        SELECT
            j.*,
            s1.neighbourhood as start_neighbourhood,
            s2.neighbourhood as end_neighbourhood
        FROM journeys AS j
        LEFT JOIN neighbourhood_stations AS s1
        ON j.start_station_name = s1.station
        LEFT JOIN neighbourhood_stations AS s2
        ON j.end_station_name = s2.station

        """
        # Run the query and return the DataFrame
        df = pd.read_sql(sql_query, self.conn)
        df.to_sql('journeys_enriched', self.conn, index=False, if_exists='append')
    
class BlueBikesDashboard:
    def __init__(self):
        self.engine = create_engine('postgresql://postgres:postgres@localhost:5432/desmondmolloy')
        # Create a connection to the engine called `conn`
        self.conn = self.engine.connect()
    def create_dash_application(self, group_by='start_neighbourhood'):
        # Create the Dash app
        app = dash.Dash(__name__)
        # Create a DataFrame from the Postgres table
        df = pd.read_sql('SELECT {}, COUNT(*) as journeys_count FROM journeys_enriched group by 1'.format(group_by), self.conn)
        # Create a bar chart of the number of trips by neighbourhood
        fig = px.bar(df, x='start_neighbourhood', y='journeys_count')
        # Create the Dash app layout
        app.layout = html.Div(children=[
            html.H1(children='Hello Dash'),
            dcc.Graph(
                id='example-graph',
                figure=fig
            )
        ])
        # Return the app
        return app
    def run(self):
        # Create the Dash app
        app = self.create_dash_application()
        # Run the app
        app.run_server(debug=True, use_reloader=False)
