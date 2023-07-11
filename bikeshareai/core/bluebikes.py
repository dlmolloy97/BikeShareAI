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
from sqlalchemy import create_engine, text
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
        
        def unzip_file_to_local_csv(self):
            # Download the zip file from the URL
            request.urlretrieve(self.url, 'data.zip')
            # Unzip the file
            ZipFile('data.zip').extractall('data')
            # Return the unzipped file
            # return 'data/tripdata.csv'
        
        def df_to_db(self, table_name, dataframe):
            # Append the data to the `trips` table
            dataframe.to_sql(table_name, self.conn, index=False, if_exists='append')
        
        def main_join(self, geojson_path, station_path, journeys_path):
            neighbourhoods = read_file(geojson_path)
            stations = pd.read_csv(station_path)
            journeys = pd.read_csv(journeys_path)
            neighbourhoods = neighbourhoods[['Name', 'geometry']]
            neighbourhoods.columns = ['neighbourhood', 'geometry']
            journeys = pd.read_csv(journeys_path)
            stations = stations[['Number', 'Name', 'Latitude', 'Longitude']]
            stations.columns = ['station_id', 'station_name', 'latitude', 'longitude']
            stations_neighbourhoods = GeoDataFrame(stations, geometry=points_from_xy(stations.longitude, stations.latitude))
            stations_neighbourhoods.set_crs(epsg='4326', inplace=True)
            stations_neighbourhoods = stations_neighbourhoods.sjoin(neighbourhoods, how="left")
            journeys_enriched = journeys.merge(stations_neighbourhoods, left_on='start_station_name', right_on='station_name', how='left')
            journeys_enriched = journeys_enriched.merge(stations_neighbourhoods, left_on='end_station_name', right_on='station_name', how='left')
            journeys_enriched = journeys_enriched.drop(['station_name_x', 'station_name_y'], axis=1)
            journeys_enriched = journeys_enriched.rename(columns={'neighbourhood_x': 'start_neighbourhood', 'neighbourhood_y': 'end_neighbourhood'})
            journeys_enriched = journeys_enriched.dropna()
            journeys_enriched['duration'] = pd.to_datetime(journeys_enriched['ended_at']) - pd.to_datetime(journeys_enriched['started_at'])
            journeys_enriched['duration'] = journeys_enriched['duration'].dt.total_seconds()
            journeys_enriched['duration'] = journeys_enriched['duration'] / 60
            journeys_enriched['duration'] = journeys_enriched['duration'].astype(int)
            journeys_enriched['journey_id'] = journeys_enriched.index
            journeys_enriched['day_of_week'] = pd.to_datetime(journeys_enriched['started_at']).dt.day_name()
            journeys_enriched['hour_of_day'] = pd.to_datetime(journeys_enriched['started_at']).dt.hour
            journeys_enriched['month_of_year'] = pd.to_datetime(journeys_enriched['started_at']).dt.month
            journeys_enriched = journeys_enriched[['journey_id', 'started_at', 'ended_at', 'duration', 'start_neighbourhood', 'end_neighbourhood', 'day_of_week', 'hour_of_day', 'month_of_year']]
            journeys_enriched.to_sql('journeys_enriched', self.conn, index=False, if_exists='replace')


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
            df = pd.read_sql(text(sql), con=self.engine.connect())
            if grouping_variable in ['start_neighbourhood', 'end_neighbourhood']:
                df = df.sort_values(by='count')
            fig = px.bar(df, x=grouping_variable, y='count')
            return fig
        
    def run(self):
        self.app.run_server(debug=True, use_reloader=False)
    
# Path: sub_benches/outbench_20230528.ipynb
