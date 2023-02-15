SELECT
  COUNT(DISTINCT Affiliated_base_number)
FROM
  `ancient-folio-347808.yellowtaxi.materialized_fhv_tripdata`;

SELECT
  COUNT(DISTINCT Affiliated_base_number)
FROM
  `ancient-folio-347808.yellowtaxi.external_fhv_tripdata`
