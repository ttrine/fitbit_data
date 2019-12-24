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
            date DATE,
            "caloriesOut" FLOAT8,
            max INT,
            min INT,
            minutes INT
        );
        ''' for table in heart_zone_tables])

    # Create the remaining tables
    query += '''
        CREATE TABLE daily_summary (
            datetime TIMESTAMP,
            calories INT,
            steps INT,
            floors INT,
            elevation INT
        );

        CREATE TABLE intraday (
            datetime TIMESTAMP,
            heart_min INT,
            calories INT,
            steps INT,
            floors INT,
            elevation INT
        );

        CREATE TABLE heart (
            datetime TIMESTAMP,
            value INT
        );

        CREATE TABLE sleep_stage (
            datetime TIMESTAMP,
            stage INT
        );

        CREATE TABLE sleep_summary (
            date DATE,
            deep FLOAT8,
            light FLOAT8,
            rem FLOAT8,
            wake FLOAT8,
            "totalMinutesAsleep" INT,
            "totalTimeInBed" INT
        );

        CREATE TABLE sleep_misc (
            date DATE,
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
