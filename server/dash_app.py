import dash
from dash import dcc, html
import plotly.graph_objects as go
import requests
from dash.dependencies import Input, Output

app = dash.Dash(__name__)

def fetch_data(endpoint, params=None):
    try:
        response = requests.get(f'http://localhost:5000/{endpoint}', params=params)
        response.raise_for_status()  
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {endpoint}: {e}")
        return []
    except ValueError as e:
        print(f"Error decoding JSON from {endpoint}: {e}")
        return []


def create_3d_scatter(data, title, collective_labels=False):
    if not data:
        return go.Figure()  
    try:
        if len(data[0]) == 6:
            regions = [row[0] if row[0] is not None else 'All Regions' for row in data]
            times = [f"{row[1]}-{row[2]}-{row[3]}" if row[1] is not None and row[2] is not None and row[3] is not None else 'All Time' for row in data]
            standards = [row[4] if row[4] is not None else 'All Standards' for row in data]
            students = [float(row[5]) for row in data]
            texts = [f'Region: {row[0] if row[0] is not None else "All Regions"}, Time: {row[1]}-{row[2]}-{row[3]} if row[1] is not None and row[2] is not None and row[3] is not None else "All Time", Standard: {row[4] if row[4] is not None else "All Standards"}, Students: {row[5]}' for row in data]
        elif len(data[0]) == 4:
            regions = [row[0] if row[0] is not None else 'All Regions' for row in data]
            times = [row[1] if row[1] is not None else 'All Time' for row in data]
            standards = [row[2] if row[2] is not None else 'All Standards' for row in data]
            students = [float(row[3]) for row in data]
            texts = [f'Region: {row[0] if row[0] is not None else "All Regions"}, Time: {row[1] if row[1] is not None else "All Time"}, Standard: {row[2] if row[2] is not None else "All Standards"}, Students: {row[3]}' for row in data]
        else:
            raise ValueError("Unexpected data format")

        if collective_labels:
            regions = ['North' if 'North' in region else 'South' if 'South' in region else 'East' if 'East' in region else 'Chennai' if 'Chennai' in region else 'Chennai' if 'Chennai' in region else 'Chennai' for region in regions]
            times = [time.split('-')[0] for time in times]  
            texts = [f'Region: {region}, Year: {time}, Standard: {standard}, Students: {student}' for region, time, standard, student in zip(regions, times, standards, students)]
    except (IndexError, ValueError) as e:
        print(f"Error processing data: {e}")
        return go.Figure()  

    fig = go.Figure()
    normalized_students = normalize(students)
    fig.add_trace(go.Scatter3d(
        x=regions,
        y=standards,
        z=times,
        mode='markers',
        marker=dict(
            size=5,
            color=normalized_students,
            colorscale='Viridis',
            opacity=0.8
        ),
        text=texts,
        hoverinfo='text',
        name=title
    ))
    return fig

def normalize(values):
    min_val = min(values)
    max_val = max(values)
    if min_val == max_val:
        return [0.5] * len(values)  
    return [(val - min_val) / (max_val - min_val) for val in values]

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div([
        dcc.Link('Cube Student Data belonging to Chennai', href='/cube_student'),
        html.Br(),
        dcc.Link('Cube Drilldown Region Data and Year (all years)', href='/cube_drilldown_region'),
        html.Br(),

        dcc.Link('Cube DrillDown with total', href='/cube_rollup_region'),
    ], style={'padding': 20}),
    html.Div(id='page-content')
])

@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/cube_student':
        cube_student_data = fetch_data('cube_sales')
        fig_cube_student = create_3d_scatter(cube_student_data, 'Cube Student Data', collective_labels=True)
        return dcc.Graph(figure=fig_cube_student)
    elif pathname == '/cube_drilldown_region':
        cube_drilldown_region_data = fetch_data('cube_drilldown_region', {'region_id': 5})  # Tamil Nadu
        fig_cube_drilldown_region = create_3d_scatter(cube_drilldown_region_data, 'Cube Drilldown Region Data')
        return dcc.Graph(figure=fig_cube_drilldown_region)
    elif pathname == '/cube_drilldown_time':
        cube_drilldown_time_data = fetch_data('cube_drilldown_time', {'year': 2024})
        fig_cube_drilldown_time = create_3d_scatter(cube_drilldown_time_data, 'Cube Drilldown Time Data')
        return dcc.Graph(figure=fig_cube_drilldown_time)
    elif pathname == '/cube_rollup_region':
        cube_rollup_region_data = fetch_data('cube_rollup_region', {'year': 2024})
        fig_cube_rollup_region = create_3d_scatter(cube_rollup_region_data, 'Cube Rollup Region Data')
        return dcc.Graph(figure=fig_cube_rollup_region)
    else:
        return '404'

if __name__ == '__main__':
    app.run_server(debug=True)