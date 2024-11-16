from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.catalog import Catalog
import argparse
from typing import Tuple
import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder
import plotly.express as px

__copyright__  = "Copyright (c) 2024 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


@st.cache_data(ttl=900, show_spinner="Loading Airline Flight Data...")
def load_data(_tbl_env: StreamTableEnvironment, database_name: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """This function creates and loads from multiple query data results
    from the `airlines.flight` Apache Iceberg Table into cache and then
    returns the query results as Pandas DataFrames from the cache.
    
    Args:
        _tbl_env (StreamTableEnvironment): is the Table Environment.
        database_name (str): is the name of the database.
        
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: is a tuple of Pandas DataFrames.
    """
    # --- Flink SQL to return the number of flights by airline, year, and month, and store the
    # --- results in a Pandas DataFrame
    airline_monthly_flights_table = _tbl_env.sql_query(f"""
                                                        select 
                                                            airline,
                                                            extract(year from to_timestamp(departure_time)) as departure_year,
                                                            extract(month from to_timestamp(departure_time)) as departure_month, 
                                                            concat(date_format(to_timestamp(departure_time), 'MM'), '-', date_format(to_timestamp(departure_time), 'MMM')) as departure_month_abbr,
                                                            count(*) as flight_count
                                                        from
                                                            {database_name}.flight
                                                        group by 
                                                            airline,
                                                            extract(year from to_timestamp(departure_time)),
                                                            extract(month from to_timestamp(departure_time)),
                                                            concat(date_format(to_timestamp(departure_time), 'MM'), '-', date_format(to_timestamp(departure_time), 'MMM'))
                                                        order by
                                                            departure_year asc,
                                                            departure_month asc;
                                                    """)
    df_airline_monthly_flights_table = airline_monthly_flights_table.to_pandas()

    # --- Flink SQL to return the top airports with the most departures by airport, airline, year,
    # --- and rank, and store the results in a Pandas DataFrame
    ranked_airports_table = _tbl_env.sql_query(f"""
                                                with cte_ranked as (
                                                    select
                                                        airline,
                                                        departure_year,
                                                        departure_airport_code,
                                                        flight_count,
                                                        ROW_NUMBER() OVER (PARTITION BY airline, departure_year ORDER BY flight_count DESC) AS row_num
                                                    from (
                                                        select
                                                            airline,
                                                            extract(year from to_timestamp(departure_time)) as departure_year,
                                                            departure_airport_code,
                                                            count(*) as flight_count
                                                        from
                                                            {database_name}.flight
                                                        group by
                                                            airline,
                                                            extract(year from to_timestamp(departure_time)),
                                                            departure_airport_code
                                                    ) tbl
                                                )
                                                select 
                                                    airline,
                                                    departure_year,
                                                    departure_airport_code, 
                                                    flight_count,
                                                    row_num
                                                from 
                                                    cte_ranked;
                                            """)
    df_ranked_airports_table = ranked_airports_table.to_pandas()

    # --- Flink SQL to return the flight data by year, and store the results in a Pandas DataFrame
    flight_table = _tbl_env.sql_query(f"""
                                      select 
                                        *, 
                                        extract(year from to_timestamp(departure_time)) as departure_year 
                                      from 
                                        {database_name}.flight
                                    """)
    df_flight_table = flight_table.to_pandas()

    # --- Return the Pandas DataFrames
    return df_airline_monthly_flights_table, df_ranked_airports_table, df_flight_table


def main(args):
    """This function reads data from an Iceberg table and displays it in Streamlit.

    Args:
        args (str): is the arguments passed to the script.
    """
    # --- Set the page configuration to wide mode
    # --- MUST BE CALLED AT THE BEGINNING OF THE SCRIPT, OTHERWISE AN EXCPETION WILL BE RAISED
    st.set_page_config(layout="wide")

    # --- Create a blank Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()

    # --- Enable checkpointing every 5000 milliseconds (5 seconds)
    env.enable_checkpointing(5000)

    #
    # Set timeout to 60 seconds
    # The maximum amount of time a checkpoint attempt can take before being discarded
    #
    env.get_checkpoint_config().set_checkpoint_timeout(60000)

    #
    # Set the maximum number of concurrent checkpoints to 1 (i.e., only one checkpoint
    # is created at a time)
    #
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)

    # --- Create a Table Environment
    tbl_env = StreamTableEnvironment.create(stream_execution_environment=env, environment_settings=EnvironmentSettings.new_instance().in_batch_mode().build())

    # --- Load Apache Iceberg catalog
    catalog = load_catalog(tbl_env, args.aws_region, args.s3_bucket_name.replace("_", "-"), "apache_kickstarter")

    # --- Print the current catalog name
    print(f"Current catalog: {tbl_env.get_current_catalog()}")

    # --- Load database
    load_database(tbl_env, catalog, "airlines")

    # --- Print the current database name
    print(f"Current database: {tbl_env.get_current_database()}")

    # --- Load the data
    df_airline_monthly_flights_table, df_ranked_airports_table, df_flight_table = load_data(tbl_env, tbl_env.get_current_database())

    # --- The title and subtitle of the app
    st.title("Apache Flink Kickstarter Dashboard")
    st.write("This Streamlit application displays data from the Apache Iceberg table created by the DataGenerator and FlightImporter Flink Apps.")

    # --- Create and fill in the two dropdown boxes used to filter data in the app
    selected_airline = st.selectbox(
        index=0, 
        label='Choose Airline:',
        options=df_flight_table['airline'].dropna().unique()
    )
    selected_departure_year = st.selectbox(
        index=0,
        label='Choose Depature Year:',
        options=df_flight_table['departure_year'].dropna().unique()
    )

    # --- Container with two sections (columns) to display the bar chart and pie chart
    with st.container(border=True):    
        col1, col2 = st.columns(2)

        with col1:
            # --- Bar chart flight count by departure month for the selected airline and year
            st.header("Airline Flights")
            st.title(f"{selected_airline} Monthly Flights in {selected_departure_year}")
            st.bar_chart(data=df_airline_monthly_flights_table[(df_airline_monthly_flights_table['departure_year'] == selected_departure_year) & (df_airline_monthly_flights_table['airline'] == selected_airline)] ,
                         x="departure_month_abbr",
                         y="flight_count",
                         x_label="Departure Month",
                         y_label="Number of Flights")
            st.write(f"This bar chart displays the number of {selected_airline} monthly flights in {selected_departure_year}.  The x-axis represents the month and the y-axis represents the number of flights.")

        with col2:
            # --- Pie chart top airports by departures for the selected airline and year
            # --- Create a slider to select the number of airports to rank
            st.header("Airport Ranking")
            st.title(f"Top {selected_departure_year} {selected_airline} Airports")
            df_filter_table = df_ranked_airports_table[(df_ranked_airports_table['airline'] == selected_airline) & (df_ranked_airports_table['departure_year'] == selected_departure_year)]
            rank_value = st.slider(label="Ranking:",
                                   min_value=3,
                                   max_value=df_filter_table['row_num'].max(), 
                                   step=1,
                                   value=3)
            fig = px.pie(df_filter_table[(df_filter_table['row_num'] <= rank_value)], 
                         values='flight_count', 
                         names='departure_airport_code', 
                         title=f"Top {rank_value} based on departures",)
            st.plotly_chart(fig, theme=None)
            st.write(f"This pie chart displays the top {rank_value} airports with the most departures for {selected_airline}.  The chart shows the percentage of flights departing from each of the top {rank_value} airports.")

    # --- Container with a section to display the flight data for the selected airline and year
    with st.container(border=True):
        # --- Grid showing all the flight data for the selected airline and year
        st.header(f"{selected_departure_year} {selected_airline} Flight Data")
        df_filter_table = df_flight_table[(df_flight_table['departure_year'] == selected_departure_year) & (df_flight_table['airline'] == selected_airline)]
        AgGrid(
            df_filter_table,
            gridOptions=GridOptionsBuilder.from_dataframe(df_flight_table).build(),
            height=300, 
            width='100%'
        )
        st.write("This table displays the flight data from the Apache Iceberg table.  The table shows the email address, departure time, departure airport code, arrival time, arrival airport code, flight number, confirmation code, airline, and departure year for each flight.")


def catalog_exist(tbl_env: StreamExecutionEnvironment, catalog_to_check: str) -> bool:
    """This method checks if the catalog exist in the environment.

    Args:
        tbl_env (StreamExecutionEnvironment): The StreamExecutionEnvironment is the context
        in which a streaming program is executed. 
        catalog_to_check (str): The name of the catalog to be checked if its name exist in the
        environment.

    Returns:
        bool: True is returned, if the catalog exist in the environment.  Otherwise, False is
        returned.
    """
    catalogs = tbl_env.list_catalogs()

    # Check if a specific catalog exists
    if catalog_to_check in catalogs:
        return True
    else:
        return False
    

def load_catalog(tbl_env: StreamExecutionEnvironment, region_name: str, bucket_name: str, catalog_name: str) -> Catalog:
    """ This method loads the catalog into the environment.
    
    Args:
        tbl_env (StreamExecutionEnvironment): The StreamExecutionEnvironment is the context
        in which a streaming program is executed. 
        region_name (str): The region where the bucket is located.
        bucket_name (str): The name of the bucket where the warehouse is located.
        catalog_name (str): The name of the catalog to be loaded into the environment.
        
    Returns:
        Catalog: The catalog object is returned if the catalog is loaded into the environment.
    """
    try:
        if not catalog_exist(tbl_env, catalog_name):
            tbl_env.execute_sql(f"""
                CREATE CATALOG {catalog_name} WITH (
                    'type' = 'iceberg',
                    'warehouse' = 's3://{bucket_name}/warehouse',
                    'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
                    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
                    'glue.skip-archive' = 'True',
                    'glue.region' = '{region_name}'
                    );
            """)
        else:
            print(f"The {catalog_name} catalog already exists.")
    except Exception as e:
        print(f"A critical error occurred to during the processing of the catalog because {e}")
        exit(1)

    # --- Use the Iceberg catalog
    tbl_env.use_catalog(catalog_name)

    # --- Access the Iceberg catalog to query the airlines database
    return tbl_env.get_catalog(catalog_name)


def load_database(tbl_env: StreamExecutionEnvironment, catalog: Catalog, database_name:str) -> None:
    """This method loads the database into the environment.

    Args:
        tbl_env (StreamExecutionEnvironment): The StreamExecutionEnvironment is the con.text
        catalog (Catalog): The catalog object is the catalog to be used to create the database.
        database_name (str): The name of the database to be loaded into the environment.
    """
    try:
        if not catalog.database_exists(database_name):
            tbl_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {database_name};")
        else:
            print(f"The {database_name} database already exists.")
        tbl_env.use_database(database_name)
    except Exception as e:
        print(f"A critical error occurred to during the processing of the database because {e}")
        exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--aws-s3-bucket',
                        dest='s3_bucket_name',
                        required=True,
                        help='The AWS S3 bucket name.')
    parser.add_argument('--aws-region',
                        dest='aws_region',
                        required=True,
                        help='The AWS Rgion name.')
    known_args, _ = parser.parse_known_args()
    main(known_args)
