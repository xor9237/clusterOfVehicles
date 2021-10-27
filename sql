WITH "results" AS
  (SELECT *
   FROM "Trips"
   WHERE "createdAt"> now() - '{{ Hours }} hours'::interval
     AND "startGeofenceId" =
       (SELECT "id"
        FROM "GeoRegions"
        WHERE "name" = '{{ GeoRegions }}' order by 1 desc limit 1)),
     "clusteredResults" AS
  (SELECT "id",
          "vehicleId", "fareAmount",
          sT_ClusterKMeans("start", {{ Number OF Clusters }}) OVER() AS cid --   ST_ClusterDBSCAN(ST_Collect(ARRAY( SELECT "start" FROM "results" )), eps := 400, minpoints := 20) OVER() AS clst_id,
FROM "results"),
     "clusteredResultsGroupedByCount" AS
  (SELECT count(DISTINCT "vehicleId") AS "numActiveVehiclesInCluster",
          count(*) AS "totalTripsInCluster", sum("fareAmount") as "sumFareOfCluster",
          "cid"
   FROM "clusteredResults"
   GROUP BY 4),
     "data" AS
  (SELECT "clusteredResults".*,
          "start",
          st_y("start"),
          st_x("start"),
          "clusteredResultsGroupedByCount"."numActiveVehiclesInCluster",
          "clusteredResultsGroupedByCount"."totalTripsInCluster",
          "clusteredResultsGroupedByCount"."totalTripsInCluster"::float / "clusteredResultsGroupedByCount"."numActiveVehiclesInCluster" AS TPAVD, "clusteredResultsGroupedByCount"."sumFareOfCluster"
   FROM "clusteredResults"
   JOIN "results" ON "clusteredResults"."id" = "results"."id"
   JOIN "clusteredResultsGroupedByCount" ON "clusteredResults"."cid" = "clusteredResultsGroupedByCount"."cid"),
     "polygons" AS
  (SELECT "cid",
          st_astext(st_concavehull(st_collect ("start"),0.99)) AS "polygon"
   FROM "data"
   GROUP BY 1),
     "dv" AS
  (SELECT "polygons"."cid",
          count(*)/{{ Hours }} AS "avgDV"
   FROM "polygons"
   JOIN "VehicleSnapshots" ON st_intersects("polygons"."polygon"::geometry, "location")
   WHERE "createdAt" > now() - '{{ Hours }} hours'::interval and ("status"= 0 or "status" = 2)
     AND "geofenceId" =
       (SELECT "id"
        FROM "GeoRegions"
        WHERE "name" = '{{ GeoRegions }}')
   GROUP BY 1)
SELECT "data".*,
       "avgDV" as "avgDV",
       "totalTripsInCluster"::float / ("avgDV") AS "TPDV", "sumFareOfCluster"::float/ "avgDV" as "rev/DV"
FROM "data"
JOIN "dv" ON "data"."cid" = "dv"."cid"
