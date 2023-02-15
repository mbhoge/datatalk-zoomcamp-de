SELECT
  *
FROM
  `ancient-folio-347808.yellowtaxi.external_fhv_tripdata`
WHERE
  DATE(pickup_datetime) BETWEEN '2019-03-01'
  AND '2019-03-31'

SELECT
  *
FROM
  `ancient-folio-347808.yellowtaxi.partitioned_fhv_tripdata`
WHERE
  DATE(pickup_datetime) BETWEEN '2019-03-01'
  AND '2019-03-31'
