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
import folium


class DashboardBikeMap:

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
    
    def create_map(self):
        # Create a Folium map
        m = folium.Map(location=[55.676098, 12.568337], zoom_start=11, tiles='cartodbpositron')
        # Create a choropleth layer showing the number of journeys starting in each neighbourhood
        folium.Choropleth(
            geo_data='data/outbench.geojson',
            name='choropleth',
            data=self.df,
            columns=['start_neighbourhood', 'number_of_journeys'],
            key_on='feature.properties.name',
            fill_color='YlGn',
            fill_opacity=0.7,
            line_opacity=0.2,
            legend_name='Number of journeys'
        ).add_to(m)
        # Create a choropleth layer showing the average duration of journeys starting in each neighbourhood
        folium.Choropleth(
            geo_data='data/outbench.geojson',
            name='choropleth',
            data=self.df,
            columns=['start_neighbourhood', 'average_duration'],
            key_on='feature.properties.name',
            fill_color='YlGn',
            fill_opacity=0.7,
            line_opacity=0.2,
            legend_name='Average duration'
        ).add_to(m)
        return m
    
    def save_map(self):
        m = self.create_map()
        m.save('map.html')

    def load_map(self):
        m = folium.Map(location=[55.676098, 12.568337], zoom_start=11, tiles='cartodbpositron')
        m = m.add_child(folium.IFrame('map.html', width=700, height=450))
        return m
