from urllib import request
import logging
import requests.exceptions as requests_exceptions


def _get_data(year, month, day, hour, output_path):
    """
    Function to download Wikipedia pageviews data for 1 hour

    Args: 
        year(str): Dag run year
        month(str): Dag run month
        day(str): Dag run day
        output_path(str): The location the file will be downloaded to

    Performs: 
        Downloads Wikipedia pageviews data
    """

    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )

    try:
        request.urlretrieve(url, output_path)
    except requests_exceptions.ConnectionError:
                print(f"Could not connect to {url}.")

    logging.info("pageviewa file downloaded to %s", output_path)


def _fetch_pageviews(pagenames, dag_directory, data_interval_start):
    """
    Function to fetch the needed data from the downloaded pageviews file and writes the insert statement to load the data to database
    
    Args: 
        pagenames(dict): Dag run year
        data_interval_start(datetime): The start datetime of the DAG run schedule interval
    
    Performs: 
        Fetch needed data and writes the insert statement to load the data to a database
    """
    #create a dictionary (result) where the keys are the values in pagenames, and the corresponding values are initialized to 0
    result = dict.fromkeys(pagenames, 0)

    with open(f"{dag_directory}/data/wikipageviews", "r") as f:
        for line in f:
            #split the line into parts wherever there is a space. The underscore is used to ignore the last part of the line.
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                #update the result dictionary so that the page title's view count is no longer 0 but the actual number of views found in the file.
                result[page_title] = view_counts

    with open(f"{dag_directory}/sql/load_pageviews.sql", "w") as f:
        for pagename, pageviewcount in result.items():
            f.write(
                "INSERT INTO pageview_counts VALUES ("
                f"'{pagename}', {pageviewcount}, '{data_interval_start}'"
                ");\n"
            )

            logging.info("Insert statement generated for %s", pagename)