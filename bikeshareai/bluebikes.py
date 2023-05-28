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
from dash.dependencies import Input, Output

class BlueBikesDataPipeline:

    def __init__(self, url):
        self.url = url
        self.engine = create_engine('postgresql://postgres:postgres@localhost:5432/desmondmolloy')
        # Create a connection to the engine called `conn`
        self.conn = self.engine.connect()

    def connect_to_db(self):
        # Create a connection to the engine called `conn`
        self.conn = self.engine.connect()
        # Return the connection
        return self.conn
    
    def unzip_file_to_local_csv(self):
        # Download the zip file from the URL
        # request.urlretrieve(self.url, 'data.zip')
        # Unzip the file
        ZipFile('data.zip').extractall('data')
        # Return the unzipped file
        # return 'data/tripdata.csv'

    def csv_to_db(self, table_name, csv_path):
        # Read in the DataFrame from the CSV file
        df = pd.read_csv(csv_path)
        # Append the data to the `trips` table
        df.to_sql(table_name, self.conn, index=False, if_exists='append')

    def enrich_journeys(self, geojson_path, trips_csv_path):
        # Read in the DataFrame from the CSV file
        df = pd.read_csv('data/current_bluebikes_stations.csv')
        # Append the data to the `trips` table
        df.to_sql('stations', self.conn, index=False, if_exists='append')
        # Read in the trips data from the trip CSV file
        trips_df = pd.read_csv(trips_csv_path)
        # Append the data to the `trips` table
        trips_df.to_sql('journeys', self.conn, index=False, if_exists='append')
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
        matched_pairs_with_pandas.to_sql('neighbourhood_stations', self.conn, index=False, if_exists='replace')
        # Had to manually prompt Copilot here
        sql_query = """
        SELECT * FROM journeys
        JOIN neighbourhood_stations
        ON journeys.start_station_name = neighbourhood_stations.station
        JOIN neighbourhood_stations AS neighbourhood_stations_end
        ON journeys.end_station_name = neighbourhood_stations_end.station
        """
        enriched_df = pd.read_sql(sql_query, self.conn)
        enriched_df['start_day'] = pd.to_datetime(enriched_df['start_time']).dt.day_name()
        enriched_df['start_hour'] = pd.to_datetime(enriched_df['start_time']).dt.hour
        enriched_df['start_month'] = pd.to_datetime(enriched_df['start_time']).dt.month
        enriched_df.to_sql('journeys_enriched', self.conn, index=False, if_exists='replace')

class DashboardBike:

    def __init__(self):
        # The line below was added manually
        self.engine = create_engine('postgresql://postgres:postgres@localhost:5432/desmondmolloy')
        self.app = dash.Dash(__name__)
        self.app.layout = html.Div([
            html.H1('Bike Dashboard'),
            html.H2('Created by Desmond Molloy'),
            dcc.Dropdown(
                id='response_variable',
                options=[
                    {'label': 'Journeys', 'value': 'journeys'},
                    {'label': 'Duration', 'value': 'duration'}
                ],
                value='journeys'
            ),
            dcc.Dropdown(
                id='grouping_variable',
                options=[
                    {'label': 'Start Neighbourhood', 'value': 'start_neighbourhood'},
                    {'label': 'End Neighbourhood', 'value': 'end_neighbourhood'},
                    {'label': 'Day of Week', 'value': 'day_of_week'},
                    {'label': 'Hour of Day', 'value': 'hour_of_day'},
                    {'label': 'Month of Year', 'value': 'month_of_year'}
                ],
                value='start_neighbourhood'
            ),
            dcc.Graph(id='bike_graph')
        ])

        @self.app.callback(
            Output('bike_graph', 'figure'),
            Input('response_variable', 'value'),
            Input('grouping_variable', 'value'))
        def update_graph(response_variable, grouping_variable):
            if response_variable == 'journeys':
                sql = 'SELECT {}, COUNT(*) as count FROM journeys_enriched group by 1'.format(grouping_variable)
            else:
                sql = 'SELECT {}, AVG(duration) as count FROM journeys_enriched group by 1'.format(grouping_variable)
            df = pd.read_sql(sql, con=self.engine)
            fig = px.bar(df, x=grouping_variable, y='count')
            return fig
        
    def run(self):
        self.app.run_server(debug=True, use_reloader=False)
    
# Path: sub_benches/outbench_20230528.ipynb
