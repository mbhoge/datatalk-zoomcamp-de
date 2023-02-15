CREATE EXTERNAL TABLE `ancient-folio-347808.yellowtaxi.external_fhv_tripdata`
OPTIONS(
	  format="CSV",
	  uris=["gs://fhv_tripdata_2019/fhv_tripdata_2019-*.csv.gz"]
);


CREATE TABLE `ancient-folio-347808.yellowtaxi.fhv_tripdata`
(
	  dispatching_base_num STRING,
	  pickup_datetime TIMESTAMP,
	  dropOff_datetime TIMESTAMP,
	  PUlocationID INT64,
	  DOlocationID INT64,
	  SR_Flag STRING,
	  Affiliated_base_number STRING
);

create or replace table `ancient-folio-347808.yellowtaxi.materialized_fhv_tripdata` as select * from `ancient-folio-347808.yellowtaxi.external_fhv_tripdata`


CREATE TABLE
  `ancient-folio-347808.yellowtaxi.partitioned_fhv_tripdata`
PARTITION BY
  DATE(pickup_datetime)
AS (
	  SELECT
	    *
	  FROM
	    `ancient-folio-347808.yellowtaxi.external_fhv_tripdata`
);
