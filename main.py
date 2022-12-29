import dash
import json
import numpy as np
import pandas as pd
import requests
import xmltodict
from dash import dash_table, dcc, html
from dash.dependencies import Input, Output


DIST = 100
LOC = 250000.0
PERSIST_TIME = 600
UPDATE_TIME = 2

app = dash.Dash(__name__)

out = []


def update_pilot_info():
    drone_pos = requests.get(
        'http://assignments.reaktor.com/birdnest/drones'
    )

    def euclidean_dist(df1, df2, cols):
        return np.linalg.norm(df1[cols].values - df2[cols].values, axis=1)

    dict_data = xmltodict.parse(drone_pos.content)
    drone_pos_df = pd.DataFrame.from_dict(dict_data['report']['capture'])
    drone_pos_df.drone.apply(pd.Series)
    drone_pos_df[['serialNumber', 'model', 'manufacturer', 'mac', 'ipv4',
                  'ipv6', 'firmware', 'positionY', 'positionX',
                  'altitude']] = drone_pos_df.drone.apply(pd.Series)
    drone_pos_df.drop('drone', axis=1)
    drone_pos_df.drop(['drone', 'model', 'manufacturer', 'mac', 'ipv4',
                       'ipv6', 'firmware', 'altitude'], axis=1, inplace=True)
    drone_pos_df['positionX'] = drone_pos_df['positionX'].astype(float)
    drone_pos_df['positionY'] = drone_pos_df['positionY'].astype(float)

    loc_df = pd.DataFrame(LOC,
                          index=list(range(0, len(drone_pos_df.index), 1)),
                          columns=['positionY', 'positionX'])
    drone_pos_df['norm_dist'] = euclidean_dist(drone_pos_df, loc_df,
                                               cols=['positionX',
                                                     'positionY']) / 1000
    drone_pos_filtered_df = drone_pos_df[drone_pos_df['norm_dist'] <= DIST]
    violators_serialnos = drone_pos_filtered_df['serialNumber'].tolist()
    pilot_info_df = pd.DataFrame(columns=['serialNumber', 'pilotId',
                                          'firstName', 'lastName',
                                          'phoneNumber', 'createdDt', 'email'])
    for sno in violators_serialnos:
        pilot_info = requests.get(
            "http://assignments.reaktor.com/birdnest/pilots/" + sno
        )

        if pilot_info.status_code == 404:
            pilot_info_df.loc[len(pilot_info_df.index)] = [sno, 'Not found',
                                                           'Not found',
                                                           'Not found',
                                                           'Not found',
                                                           'Not found',
                                                           'Not found']
        else:
            pilot_info_dict = json.loads(pilot_info.content.decode('utf-8'))
            pilot_info_df.loc[len(pilot_info_df.index)] = [sno,
                                                           pilot_info_dict['pilotId'],
                                                           pilot_info_dict['firstName'],
                                                           pilot_info_dict['lastName'],
                                                           pilot_info_dict['phoneNumber'],
                                                           pilot_info_dict['createdDt'],
                                                           pilot_info_dict['email']]
    drone_pilot_merge_df = pd.merge(drone_pos_filtered_df, pilot_info_df,
                                    on='serialNumber')
    drone_pilot_merge_df.drop(['serialNumber', 'positionY', 'positionX',
                               'pilotId', 'createdDt'], axis=1, inplace=True)

    drone_pilot_merge_df['time_stamp'] = pd.to_datetime(drone_pilot_merge_df['@snapshotTimestamp'])

    out.append(drone_pilot_merge_df)
    out_df = pd.concat(out)
    out_df.drop_duplicates()

    if len(out_df.index) > 1:
        out_df['diff'] = (out_df['time_stamp'].iloc[-1]
                          - out_df['time_stamp']).dt.total_seconds()
        out_df = out_df[out_df['diff'] <= PERSIST_TIME]
    else:
        out_df['diff'] = 0
    out_df['min_dist'] = out_df.groupby(['email']).norm_dist.transform('min')

    out_df['Name'] = out_df['firstName'] + ' ' + out_df['lastName']
    out_df = out_df.rename(columns={'min_dist': 'Min Conf. Dist. (m)',
                                    "time_stamp": "Drone seen at (UTC)",
                                    "phoneNumber": "Phone", "email": "Email"})
    out_df.drop(['firstName', 'lastName', 'norm_dist', 'diff',
                 '@snapshotTimestamp'], axis=1, inplace=True)
    out_df = out_df[['Name', 'Email', 'Phone', 'Drone seen at (UTC)',
                     'Min Conf. Dist. (m)']]
    out_df['Min Conf. Dist. (m)'] = out_df['Min Conf. Dist. (m)'].round(2)

    return out_df


app.layout = html.Div([
    html.H1(children="BIRDNEST: No Drone Zone Detection System",
            style={'color': '#00361c', 'text-align': 'center'}),
    dcc.Interval(
                id='my_interval',
                disabled=False,
                n_intervals=0,
                interval=UPDATE_TIME*1000
    ),
    html.Div([
        html.Div(id="pilot_info")
    ]),

])


@app.callback(Output("pilot_info", "children"),
              [Input("my_interval", "n_intervals")])
def update_pilot_info_div(n):
    info_df = update_pilot_info()

    return dash_table.DataTable(
        id='table',
        columns=[{"name": i, "id": i}
                 for i in info_df.columns],
        data=info_df.to_dict('records'),
        style_cell=dict(textAlign='left'),
        style_header=dict(backgroundColor="paleturquoise"),
        style_data=dict(backgroundColor="lavender")
    )


if __name__ == '__main__':
    app.run_server(debug=True)

