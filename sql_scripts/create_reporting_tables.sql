-- create_reporting_tables.sql
-- Example SQL to create an aggregated reporting table (for Azure Synapse / SQL)
CREATE TABLE IF NOT EXISTS dbo.Covid_Daily_Summary
(
    CountryName VARCHAR(200),
    CountryISO VARCHAR(10),
    ReportDate DATE,
    NewConfirmed BIGINT,
    TotalConfirmed BIGINT,
    NewDeaths BIGINT,
    TotalDeaths BIGINT,
    NewRecovered BIGINT,
    TotalRecovered BIGINT
);

-- Example aggregation (to be run after loading parquet/CSV into a staging table/external table)
SELECT CountryName, CountryISO, ReportDate,
       SUM(NewConfirmed) AS NewConfirmed,
       SUM(NewDeaths) AS NewDeaths,
       SUM(NewRecovered) AS NewRecovered
INTO dbo.Covid_Agg_by_Country_Date
FROM dbo.Covid_Daily_Summary
GROUP BY CountryName, CountryISO, ReportDate;
