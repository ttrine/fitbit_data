from sqlalchemy import create_engine


def drop_all(engine):
    """ Drop and recreate the public schema. """
    query = '''
        DROP SCHEMA public CASCADE;
        CREATE SCHEMA public;
    '''
    return engine.execute(query)


def create_schema(engine):
    # Create heart zone tables
    heart_zone_tables = ['oor', 'fat_burn', 'cardio', 'peak']

    query = '\n'.join([f'''
        CREATE TABLE daily_{table} (
            date DATE PRIMARY KEY,
            "caloriesOut" FLOAT8,
            max INT,
            min INT,
            minutes INT
        );
        ''' for table in heart_zone_tables])

    # Create the remaining tables
    query += '''
        CREATE TABLE daily_summary (
            datetime TIMESTAMP PRIMARY KEY,
            calories INT,
            steps INT,
            floors INT,
            elevation INT
        );

        CREATE TABLE intraday (
            datetime TIMESTAMP PRIMARY KEY,
            heart_min INT,
            calories INT,
            steps INT,
            floors INT,
            elevation INT
        );

        CREATE TABLE heart (
            datetime TIMESTAMP PRIMARY KEY,
            value INT
        );

        CREATE TABLE sleep_stage (
            datetime TIMESTAMP PRIMARY KEY,
            stage INT
        );

        CREATE TABLE sleep_summary (
            date DATE PRIMARY KEY,
            deep FLOAT8,
            light FLOAT8,
            rem FLOAT8,
            wake FLOAT8,
            "totalMinutesAsleep" INT,
            "totalTimeInBed" INT
        );

        CREATE TABLE sleep_misc (
            date DATE PRIMARY KEY,
            "awakeCount" INT,
            "awakeDuration" INT,
            "awakeningsCount" INT,
            efficiency INT,
            "minutesToFallAsleep" INT,
            "restlessCount" INT,
            "restlessDuration" INT
        );
    '''
    return engine.execute(query)


if __name__ == '__main__':
    """ Drop all tables and recreate them. """

    engine = create_engine('postgresql://postgres:postgres@localhost:6543/fitbit')
    drop_result = drop_all(engine)
    create_result = create_schema(engine)
    print(drop_result, create_result)
