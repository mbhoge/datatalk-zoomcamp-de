SELECT
  COUNT(1)
FROM
  `ancient-folio-347808.yellowtaxi.external_fhv_tripdata`
WHERE
  PUlocationID IS NULL
  AND DOlocationID IS NULL
