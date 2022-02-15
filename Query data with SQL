/*
Full tutorial available on: https://docs.microsoft.com/en-us/azure/synapse-analytics/sql/tutorial-data-analyst
In this tutorial, you learn how to perform exploratory data analysis by combining different Azure Open Datasets
using serverless SQL pool and then visualizing the results in Azure Synapse Studio.

In particular, you analyze the New York City (NYC) Taxi dataset that includes:

 - Pickup and drop-off dates and times.
 - Pick up and drop-off locations.
 - Trip distances.
 - Itemized fares.
 - Rate types.
 - Payment types.
 - Driver-reported passenger counts.*/


/*
 * * * * * * * * * * * * * * * *
 * Automatic schema inference  *
 * * * * * * * * * * * * * * * *

Since data is stored in the Parquet file format, automatic schema inference is available.
You can easily query the data without listing the data types of all columns in the files.
You also can use the virtual column mechanism and the filepath function to filter out a certain subset of files.

Let's first get familiar with the NYC Taxi data by running the following query. */

SELECT TOP 100 * FROM
    OPENROWSET(
        BULK 'https://azureopendatastorage.blob.core.windows.net/nyctlc/yellow/puYear=2019/puMonth=*/*.parquet',
        FORMAT='PARQUET'
    ) AS [nyc];


/* Similarly, you can query the Public Holidays dataset by using the following query. */

SELECT TOP 100 * FROM
    OPENROWSET(
        BULK 'https://azureopendatastorage.blob.core.windows.net/holidaydatacontainer/Processed/*.parquet',
        FORMAT='PARQUET'
    ) AS [holidays];

/* Lastly, you can also query the Weather Data dataset by using the following query. */

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://azureopendatastorage.blob.core.windows.net/isdweatherdatacontainer/ISDWeather/year=2018/month=*/*.parquet',
        FORMAT='PARQUET'
    ) AS [weather];

/* You can learn more about the meaning of the individual columns in the descriptions
of the NYC Taxi, Public Holidays, and Weather Data datasets on the Azure Opendatasets page. */


/*
 * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Time series, seasonality, and outlier analysis  *
 * * * * * * * * * * * * * * * * * * * * * * * * * *
You can easily summarize the yearly number of taxi rides by using the following query. */

SELECT
    YEAR(tpepPickupDateTime) AS current_year,
    COUNT(*) AS rides_per_year
FROM
    OPENROWSET(
        BULK 'https://azureopendatastorage.blob.core.windows.net/nyctlc/yellow/puYear=*/puMonth=*/*.parquet',
        FORMAT='PARQUET'
    ) AS [nyc]
WHERE nyc.filepath(1) >= '2014' AND nyc.filepath(1) <= '2019'
GROUP BY YEAR(tpepPickupDateTime)
ORDER BY 1 ASC;

/* The data can be visualized in Synapse Studio by switching from the Table to the Chart view.
You can choose among different chart types, such as Area, Bar, Column, Line, Pie, and Scatter.
In this case, plot the Column chart with the Category column set to current_year.

From this visualization, a trend of a decreasing number of rides over years can be clearly seen.
Presumably, this decrease is due to the recent increased popularity of ride-sharing companies.
*/


/* For full tutorial click on "?" in top right corner then Knowledge center/Browse Available Samples/SQL Scripts/Analyze Azure Open Datasets using serverless SQL pool*/
