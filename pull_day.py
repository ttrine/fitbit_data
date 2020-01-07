""" Pull yesterday's data and attempt to push it to the db """

import datetime
from dateutil.parser import parse
from fitbit import Fitbit
import pickle as pkl
import pandas as pd
from sqlalchemy import create_engine


def authenticate(client_id, client_secret):
    """ Create an authenticated client from client_id, client_secret """
    token_dict = pkl.load(open('data/access_refresh.pkl', 'rb'))
    access_token = token_dict['access_token']
    refresh_token = token_dict['refresh_token']

    auth2_client = Fitbit(client_id, client_secret,
                          oauth2=True,
                          access_token=access_token,
                          refresh_token=refresh_token,
                          refresh_cb=lambda x: pkl.dump(x, open('data/access_refresh.pkl', 'wb'))
                          )

    # Ensure the latest token is written to disk
    auth2_client.client.refresh_token()

    return auth2_client


def get_intraday(client, resource, interval, date):
    return client.intraday_time_series(
        resource = f'activities/{resource}',
        base_date=date,
        detail_level=f'1{interval}'
    )


def get_raw(yesterday, client):
    # Hold all available data in one dict
    return {'heart_sec': get_intraday(client, 'heart', 'sec', yesterday),
            'heart_min': get_intraday(client, 'heart', 'min', yesterday),
            'calories': get_intraday(client, 'calories', 'min', yesterday),
            'steps': get_intraday(client, 'steps', 'min', yesterday),
            'floors': get_intraday(client, 'floors', 'min', yesterday),
            'elevation': get_intraday(client, 'elevation', 'min', yesterday),
            'sleep': client.get_sleep(yesterday)
            }


def construct_daily_summary_table(raw, yesterday):
    index = [yesterday]

    data = {
        activity: int(raw[activity][f'activities-{activity}'][0]['value'])
        for activity in ('calories', 'steps', 'floors', 'elevation')
    }

    df = pd.DataFrame(data=data, index=index)
    df.index.name = 'datetime'

    return df


def construct_daily_heart_tables(raw, yesterday):
    index = [yesterday]

    dfs = {}
    for zone, table_name in enumerate(
            ('daily_oor', 'daily_fat_burn', 'daily_cardio', 'daily_peak')):
        data = {
            col: [raw['heart_sec'][f'activities-heart'][0]['value']['heartRateZones'][0][col]]
            for col in ('caloriesOut', 'max', 'min', 'minutes')
        }
        dfs[table_name] = pd.DataFrame(data=data, index=index)
        dfs[table_name].index.name = 'date'

    return dfs


def construct_sleep_summary_table(raw, yesterday):
    index = [yesterday]

    data = {
        'deep': [],
        'light': [],
        'rem': [],
        'wake': [],
        'totalMinutesAsleep': [],
        'totalTimeInBed': []
    }

    [value.append(raw['sleep']['summary'][key])
     if key in ('totalMinutesAsleep', 'totalTimeInBed')
     else (value.append(raw['sleep']['summary']['stages'][key])
           if 'stages' in raw['sleep']['summary'].keys()
           else value.append(None)
           )
     for key, value in data.items()
     ]

    df = pd.DataFrame(data=data, index=index)
    df.index.name = 'date'

    return df


def construct_sleep_stage_table(raw, yesterday):
    sleep_by_minute = [{
        'datetime': datetime.datetime.combine(
            yesterday,
            (parse(record['dateTime'])).time()
        ),
        'stage': int(record['value'])
    }
        for sleep in raw['sleep']['sleep']
        for record in sleep['minuteData']
    ]

    index = [record['datetime'] for record in sleep_by_minute]
    data = [record['stage'] for record in sleep_by_minute]

    df = pd.DataFrame(data={'stage': data}, index=index)
    df.index.name = 'datetime'

    return df


def construct_sleep_misc_table(raw, yesterday):
    index = [yesterday]

    data = {
        'awakeCount': [],
        'awakeDuration': [],
        'awakeningsCount': [],
        'efficiency': [],
        'minutesToFallAsleep': [],
        'restlessCount': [],
        'restlessDuration': []
    }

    if len(raw['sleep']['sleep']) == 0:
        # Create empty dataframe
        df = pd.DataFrame(columns=[k for k in data])
    else:
        [value.append(sleep[key])
         for key, value in data.items()
         for sleep in raw['sleep']['sleep']
         ]

        df = pd.DataFrame(data=data, index=index)

    df.index.name = 'date'

    return df


def construct_heart_table(raw, yesterday):
    # Heartrate at the one-second resolution
    index = [datetime.datetime.combine(yesterday, (parse(record['time'])).time())
             for record in raw['heart_sec']['activities-heart-intraday']['dataset']
             ]

    data = {
        'value': [record['value']
                  for record in raw['heart_sec']['activities-heart-intraday']['dataset']
                  ]
    }

    df = pd.DataFrame(data=data, index=index)
    df.index.name = 'datetime'

    return df


def construct_intraday(raw, yesterday):
    # Put the intraday data in a single df
    # This is done by stitching together individual Series objects,
    # -> because the heart data has a different number of values
    series_objs = {}

    for key in raw:
        if key not in ('heart_sec', 'sleep'):
            # Different (coarser) indexes for this data
            # Generate a different one for each in case the number of records is different

            index = [datetime.datetime.combine(yesterday, (parse(record['time'])).time())
                     for record in (raw[key][f'activities-{key}-intraday']['dataset']
                                    if key != 'heart_min'
                                    else raw[key][f'activities-heart-intraday']['dataset']
                                    )
                     ]

            data = [record['value']
                    for record in (raw[key][f'activities-{key}-intraday']['dataset']
                                   if key != 'heart_min'
                                   else raw[key][f'activities-heart-intraday']['dataset']
                                   )
                    ]

            series_objs[key] = pd.Series(data=data, index=index)

    df = pd.DataFrame(series_objs)
    df.index.name = 'datetime'

    return df


def append_to_db_tables(con_string, daily_summary_df, daily_heart_dfs, sleep_summary_df,
                        sleep_stage_df, sleep_misc_df, heart_df, intraday_df):
    # Create database connection
    engine = create_engine(con_string)

    # Upload the frames
    sleep_stage_df.to_sql(name='sleep_stage', con=engine, if_exists='append')
    heart_df.to_sql(name='heart', con=engine, if_exists='append')
    intraday_df.to_sql(name='intraday', con=engine, if_exists='append')
    daily_summary_df.to_sql(name='daily_summary', con=engine, if_exists='append')
    [df.to_sql(name=table_name, con=engine, if_exists='append')
     for table_name, df in daily_heart_dfs.items()
     ]
    sleep_summary_df.to_sql(name='sleep_summary', con=engine, if_exists='append')
    sleep_misc_df.to_sql(name='sleep_misc', con=engine, if_exists='append')


def run(day, client_id, client_secret, con_string, load_from_disk=False):
    auth2_client = authenticate(client_id, client_secret)

    # TODO: Remove temporary debugging if-else shim below
    if load_from_disk:
        raw = pkl.load(open('data/latest_raw.pkl', 'rb'))
    else:
        raw = get_raw(day, auth2_client)

        # Save the latest to disk just in case
        pkl.dump(raw, open('data/latest_raw.pkl','wb'))

    daily_summary_df = construct_daily_summary_table(raw, day)
    daily_heart_dfs = construct_daily_heart_tables(raw, day)
    sleep_summary_df = construct_sleep_summary_table(raw, day)
    sleep_stage_df = construct_sleep_stage_table(raw, day)
    sleep_misc_df = construct_sleep_misc_table(raw, day)
    heart_df = construct_heart_table(raw, day)
    intraday_df = construct_intraday(raw, day)

    append_to_db_tables(con_string,
                        daily_summary_df,
                        daily_heart_dfs,
                        sleep_summary_df,
                        sleep_stage_df,
                        sleep_misc_df,
                        heart_df,
                        intraday_df
                        )


if __name__ == '__main__':
    """ Pull yesterday's data. """
    import os

    client_id, client_secret = os.environ['CLIENT_ID'], os.environ['CLIENT_SECRET']
    con_string = os.environ['CON_STRING']

    day = (datetime.datetime.now() - datetime.timedelta(days=1)).date()

    try:
        run(day, client_id, client_secret, con_string, load_from_disk=False)
    except Exception as e:
        # Send an email to myself so's I can fix the problem
        print("Failure")
