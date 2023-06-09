
# Create a class called DashboardBikesFolium. All methods should use typing in their arguments and returned object, and include both docstrings and line-by-line comments.
# The class should have the following methods:
# __init__: Takes argument geojson_path (string). Creates an instance of sqlalchemy.create_engine, then creates a Dash app that displays the number of trips by start and end neighbourhood, from the journeys_enriched table. The application should have one bar graph, two instances of dcc.Dropdown, and one Folium map.
# The first instance of dcc.Dropdown should be called response_variable
# The second instance should be called grouping_variable
# If response_variable = journeys, then pass a SQL statement that counts all records by grouping_variable, and creates a pandas data frame called response_df
# If response_variable = duration, then pass a SQL statement that selects the mean duration group by grouping variable, and creates a pandas data frame called response_df
# Grouping variable should be in the following:
# start_neighbourhood
# end_neighbourhood
# day_of_week
# hour_of_day
# month_of_year
# The application should then show the response variable by grouping variable in the bar graph

# The map should be a Folium map
# To create this map, the application should use the geopandas package to load the dataset from the geojson_path argument as a data frame called neighbourhoods_geo, rename the columns of neighbourhoods_geo to „Name“ and „Geometry“, 
# If grouping_variable in [‚start_neighbourhood“, „end_neighbourhood“], then create a dataframe called response_geo_df by joining response_df to neighbourhoods_geo on the grouping_variable column. Then create a choropleth layer on the map showing the response variable by grouping variable. If response_variable = journeys, then the choropleth layer should be called „Number of Journeys“. If response_variable = duration, then the choropleth layer should be called „Average Duration“.
# If grouping variable = start_neighbourhood, then the choropleth layer should be coloured by the number of journeys or average duration starting in each neighbourhood
# If grouping variable = end_neighbourhood, then the choropleth layer should be coloured by the number of journeys or average duration ending in each neighbourhood
# If the grouping variable is not start_neighbourhood or end_neighbourhood, then the choropleth layer should not be coloured
# The map should have a title called "Start and End Neighbourhoods"
# The map should have a subtitle called "Created by Desmond Molloy"
# The map should have a legend showing the start and end neighbourhoods
# The map should have a legend title called "Neighbourhoods"
# The map should have a legend subtitle called "Created by Desmond Molloy"

# - run: Calls create_dash_application and runs the Dash application on the local server


class DashboardBikesFolium:
    '''
    Creates a dashboard that displays the number of trips by start and end neighbourhood, from the journeys_enriched table. The application should have one bar graph, two instances of dcc.Dropdown, and one Folium map.
    '''

    def __init__(self, geojson_path: str):
        '''
        Creates an instance of sqlalchemy.create_engine, then creates a Dash app that displays the number of trips by start and end neighbourhood, from the journeys_enriched table. The application should have one bar graph, two instances of dcc.Dropdown, and one Folium map.
        
        Arguments:
            geojson_path: Path to geojson file
        
        Returns:
            None
        '''
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
        '''
        Runs the Dash application on the local server
        
        Arguments:
            None
        
        Returns:
            None
        '''
        self.app.run_server(debug=True, use_reloader=False)

    

        