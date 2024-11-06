# Hive HDR UDAF

> [!NOTE]
> This is a work in progress.
> Please feel free to reach out to the author if you have any questions or comments

HDR Histogram is a very useful library for analyzing data samples that have a long tail where it is useful to understand its behaviour in this long tail; e.g. at 99.9%, 99.99%, 99.999%, etc.  Latency values are one common example where HDR Histograms can shed light on the characteristics of the long tail.

This is a Hive User Defined Aggregation Function (UDAF) Plugin that uses the HDR Histogram library.
It provides two different HDR functions: one that returns a nested Array of Structs that can be visualized, and another that returns a Histogram object encoded into a base64 string.

## Notes on HDR Histograms
HDR Histograms are commutative.  This means that a two histograms, each for a separate 15-minute period, could be added together, and produce the same result as a single histogram covering the two 15-minute periods.  This enables all kinds of interesting properties.
HDR Histograms also have a base64-encoding scheme that allows them to be persisted, transmitted, and/or re-constructed.

In combination, these two characteristics combine to create some interesting potential use cases.

 * Map/Reduce: Histograms can be leveraged in parallelized map/reduce operations to work on large data sets.
 * Rollups: several shorter-granularity histograms could be aggregated together into a daily histogram.  
 * Logging: If a system generates a base64-encoded histogram from its direct measurements, this could then be used to re-construct the histogram and add it with others.  
 * Rollups of histograms measured elsewhere: Logs containing HDR histograms could be generated from active elements, and periodically logged to a central system.  They could then be stored, and then re-combined with other HDR histograms coming from different processes, pods, measurement points, etc.  They could also then be rolled up to a coarser level of granularity.


## Functions
Hive User Defined Functions are assigned a name when they are loaded.  For clarity, the function names used here assume that the functions have been named hdr_histogram and encode_hdr_histogram.

### hdr_histogram function

This function can accept and generate HDR Histograms for positive discrete numeric values: byte, short, ints, and longs. 
It can also accept string values that are base64-encoded histograms.  

This function returns an array of structs with four values.  
The values are calculated in the same way that the HDR Histogram library calculates its string-based output.  the four attributes are:
 * value: the value of the attribute that is being analyzed
 * percentile: the percentile represented by this value
 * totalcount: the cumulative number of samples that are within this percentile (e.g. from 0% to this percentile value)
 * pctlogscale: a log10-scaled transformation of the percentile value that aids in visualizing the output

The beeline command displays this complex object result as a json struct.
```
[{"value":50.0,"percentile":0.0,"totalcount":52691.0,"pctlogscale":1.0},
{"value":51.0,"percentile":0.1,"totalcount":105029.0,"pctlogscale":1.1111111111111112},
{"value":53.0,"percentile":0.2,"totalcount":207926.0,"pctlogscale":1.25},
{"value":55.0,"percentile":0.3,"totalcount":307060.0,"pctlogscale":1.4285714285714286},
{"value":57.0,"percentile":0.4,"totalcount":401018.0,"pctlogscale":1.6666666666666667},
[...]
]
```

### encode_hdr_histogram function
This function can accept and generate HDR Histograms for positive discrete numeric values: byte, short, ints, and longs. 
It can also accept string values that are base64-encoded histograms.  This may prove useful in having granular histograms (e.g. per-object, or per-granular-time-period), and then aggregating several of the HDR Histograms together into one.  For example, as data ages, several 1-minute HDR Histograms
could be aggregated together to preserve the data at an hourly or daily granularity.

This function returns the HDR histogram encoded into a base64 string, suitable for storing, re-aggregation, or visualization.  Below is an example of a base64-encoded histogram. 
```
HISTFAAAAXZ4nC1QvUoDQRDem5tMNuO6npfjvOI4gkoQCViIjYhKGrUQf15BS0sfQeyNPoJVELFIISJWQURELMUHk
BRXWAQLEXX2zllmdv52v28mPTqtK+Xdq1Lg//bKa+x35aP09s5237cGO5/bD5v55vfG6/rN2ls7X/1Z6i8+L+Tzl3
PXrZfZYTOf6jUG6VdynBxNdOtX4/2x89G7kS7ntX51SBeVR+xhBzv+AD69W+/JO/YOD/bby62ZJIoC0gSoREdAiAB
UASrgg4gPCsWgxE4kACp7CiV5RCgpRiREEGOQmFkzMCGTtsjaiAeGDbPVxkbGsg05YmtMiIa0juPASgZNkk3GzTht
NmYmkzTLphtxkoZhFsRBFBoKA6NZxygvyBotMO5jFHBG5fBBeEJh0DlKK1Ss0BdmiNIvXYGrGCJpKZijlhnEyAigL
KEWksr9rWQhMmINyvFBkcSSKEHQLxAq5EPRKOW625m4VXBag2J/J26X7vwBEcI59A==
```

## Example Usage
These instructions help demonstrate the HDR Histogram UDAF in a small (desktop/laptop) type environment.

Run Hive locally
```
export HIVE_VERSION=3.1.3
docker pull apache/hive:$HIVE_VERSION

docker run -d -p 9083:9083 --env SERVICE_NAME=metastore --name metastore-standalone apache/hive:${HIVE_VERSION}
docker run -d -p 10000:10000 -p 10002:10002 --env SERVICE_NAME=hiveserver2 --name hiveserver2 apache/hive:${HIVE_VERSION}
```

Create sample data and copy it into the running docker container (this isn't best practice in any way, but is useful in this demonstration)
```
awk -v OFS=, '{print $0, NR % 10}' src/test/resources/samples-1m.txt > /tmp/samples-1m-partitioned.csv

docker cp /tmp/samples-1m-partitioned.csv hiveserver2:/tmp
```

Copy the UDAF jar into the container
```
docker cp target/hive-hdr-udaf-1.0-SNAPSHOT.jar hiveserver2:tmp
```

Run the beeline hive commandline
```
docker exec -it hiveserver2 beeline -u 'jdbc:hive2://localhost:10000/'
```

Add the UDAF functions using ```add jar```
```
0: jdbc:hive2://localhost:10000/> add jar file:///tmp/hive-hdr-udaf-1.0-SNAPSHOT.jar;
No rows affected (0.087 seconds)
```
Validate that it has been added (and there's no issues accessing the jar file)
```
0: jdbc:hive2://localhost:10000/> list jars;
+---------------------------------------------+
|                  resource                   |
+---------------------------------------------+
| file:///tmp/hive-hdr-udaf-1.0-SNAPSHOT.jar  |
+---------------------------------------------+
1 row selected (0.063 seconds)
```

Create the temporary function entries for the hdr_histogram UDAF
```
0: jdbc:hive2://localhost:10000/> create temporary function hdr_histogram;
No rows affected (0.77 seconds)
```

Create the temporary function entry for encode_hdr_histogram
```
0: jdbc:hive2://localhost:10000/> create temporary function encode_hdr_histogram as 'net.edmison.HdrHistogram.hive.udaf.GenericUDAFEncodedHdrHistogram';
No rows affected (0.018 seconds)
```
Show function help.  This validates that Hive is able to invoke the supplied jar file.
```
0: jdbc:hive2://localhost:10000/> describe function hdr_histogram;
+----------------------------------------------------+
|                      tab_name                      |
+----------------------------------------------------+
| hdr_histogram(x, precision) - Returns a HDR Histogram JSON of a set of numbers |
+----------------------------------------------------+
1 row selected (0.163 seconds)

0: jdbc:hive2://localhost:10000/> describe function encode_hdr_histogram;
+----------------------------------------------------+
|                      tab_name                      |
+----------------------------------------------------+
| encode_hdr_histogram(x, precision) - Returns a HDR Histogram JSON of a set of numbers |
+----------------------------------------------------+
1 row selected (0.021 seconds)


0: jdbc:hive2://localhost:10000/> describe function extended hdr_histogram;
+----------------------------------------------------+
|                      tab_name                      |
+----------------------------------------------------+
| hdr_histogram(x, precision) - Returns a HDR Histogram JSON of a set of numbers |
| Example:                                           |
| SELECT hdr_histogram(val, 3) from src;             |
| This creates a HDR histogram for the specified set, with the specified result precision |
| Function class:net.edmison.HdrHistogram.hive.udaf.GenericUDAFHdrHistogram |
| Function type:TEMPORARY                            |
+----------------------------------------------------+

0: jdbc:hive2://localhost:10000/> describe function extended encode_hdr_histogram;
+----------------------------------------------------+
|                      tab_name                      |
+----------------------------------------------------+
| encode_hdr_histogram(x, precision) - Returns a HDR Histogram JSON of a set of numbers |
| Example:                                           |
| SELECT hdr_histogram(val, 3) from src;             |
| This creates a HDR histogram for the specified set, with the specified result precision |
| Function class:net.edmison.HdrHistogram.hive.udaf.GenericUDAFEncodedHdrHistogram |
| Function type:TEMPORARY                            |
+----------------------------------------------------+
6 rows selected (0.02 seconds)
```

Beeline commands to put the sample data into a table
```
0: jdbc:hive2://localhost:10000/> CREATE TABLE IF NOT EXISTS sample_data(value int) partitioned by(b int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
No rows affected (0.129 seconds)


0: jdbc:hive2://localhost:10000/> load data local inpath '/tmp/samples-1m-partitioned.csv' overwrite into table sample_data;
No rows affected (3.365 seconds)
```

Beeline commands to demonstrate some basic access to the sample data
```
0: jdbc:hive2://localhost:10000/> select b, count(*) from sample_data group by b;
+----+---------+
| b  |   _c1   |
+----+---------+
| 0  | 100000  |
| 1  | 100000  |
| 2  | 100000  |
| 3  | 100000  |
| 4  | 100000  |
| 5  | 100000  |
| 6  | 100000  |
| 7  | 100000  |
| 8  | 100000  |
| 9  | 100000  |
+----+---------+
10 rows selected (1.71 seconds)

0: jdbc:hive2://localhost:10000/> select * from sample_data limit 5;
+--------------------+----------------+
| sample_data.value  | sample_data.b  |
+--------------------+----------------+
| 66                 | 0              |
| 58                 | 0              |
| 60                 | 0              |
| 56                 | 0              |
| 77                 | 0              |
+--------------------+----------------+
5 rows selected (0.071 seconds)
```
Beeline commands to demonstrate the hdr_histogram UDAF, and then expanding its array of structs response.
```
0: jdbc:hive2://localhost:10000/> select hdr_histogram(value, 2) from sample_data;
+----------------------------------------------------+
|                        _c0                         |
+----------------------------------------------------+
| [{"value":50.0,"percentile":0.0,"totalcount":52691.0,"pctlogscale":1.0},{"value":51.0,"percentile":0.1,"totalcount":105029.0,"pctlogscale":1.1111111111111112},{"value":53.0,"percentile":0.2,"totalcount":207926.0,"pctlogscale":1.25},{"value":55.0,"percentile":0.3,"totalcount":307060.0,"pctlogscale":1.4285714285714286},{"value":57.0,"percentile":0.4,"totalcount":401018.0,"pctlogscale":1.6666666666666667},{"value":60.0,"percentile":0.5,"totalcount":529906.0,"pctlogscale":2.0},{"value":61.0,"percentile":0.55,"totalcount":569248.0,"pctlogscale":2.2222222222222223},{"value":62.0,"percentile":0.6,"totalcount":606422.0,"pctlogscale":2.5},{"value":64.0,"percentile":0.65,"totalcount":674614.0,"pctlogscale":2.857142857142857},{"value":65.0,"percentile":0.7,"totalcount":705215.0,"pctlogscale":3.333333333333333},{"value":67.0,"percentile":0.75,"totalcount":760748.0,"pctlogscale":4.0},{"value":68.0,"percentile":0.775,"totalcount":785525.0,"pctlogscale":4.444444444444445},{"value":69.0,"percentile":0.8,"totalcount":808228.0,"pctlogscale":5.000000000000001},{"value":70.0,"percentile":0.825,"totalcount":828819.0,"pctlogscale":5.714285714285713},{"value":72.0,"percentile":0.85,"totalcount":864792.0,"pctlogscale":6.666666666666666},{"value":73.0,"percentile":0.875,"totalcount":880094.0,"pctlogscale":8.0},{"value":74.0,"percentile":0.8875,"totalcount":893834.0,"pctlogscale":8.888888888888886},{"value":75.0,"percentile":0.9,"totalcount":906067.0,"pctlogscale":10.000000000000002},{"value":76.0,"percentile":0.9125,"totalcount":917009.0,"pctlogscale":11.428571428571425},{"value":77.0,"percentile":0.925,"totalcount":926682.0,"pctlogscale":13.333333333333341},{"value":79.0,"percentile":0.9375,"totalcount":942976.0,"pctlogscale":16.0},{"value":80.0,"percentile":0.94375,"totalcount":949604.0,"pctlogscale":17.77777777777777},{"value":81.0,"percentile":0.95,"totalcount":955563.0,"pctlogscale":19.999999999999982},{"value":82.0,"percentile":0.95625,"totalcount":960588.0,"pctlogscale":22.85714285714288},{"value":83.0,"percentile":0.9625,"totalcount":964944.0,"pctlogscale":26.666666666666682},{"value":85.0,"percentile":0.96875,"totalcount":971799.0,"pctlogscale":32.0},{"value":86.0,"percentile":0.971875,"totalcount":974612.0,"pctlogscale":35.555555555555614},{"value":87.0,"percentile":0.975,"totalcount":977013.0,"pctlogscale":39.999999999999964},
[...some entries removed...]
{"value":2271.0,"percentile":0.9999990463256836,"totalcount":1000000.0,"pctlogscale":1048576.0}] |
+----------------------------------------------------+
1 row selected (0.84 seconds)
```

```
0: jdbc:hive2://localhost:10000/> select explode(hdr_histogram(value, 2)) from sample_data;
+----------------------------------------------------+
|                        col                         |
+----------------------------------------------------+
| {"value":50.0,"percentile":0.0,"totalcount":52691.0,"pctlogscale":1.0} |
| {"value":51.0,"percentile":0.1,"totalcount":105029.0,"pctlogscale":1.1111111111111112} |
| {"value":53.0,"percentile":0.2,"totalcount":207926.0,"pctlogscale":1.25} |
| {"value":55.0,"percentile":0.3,"totalcount":307060.0,"pctlogscale":1.4285714285714286} |
| {"value":57.0,"percentile":0.4,"totalcount":401018.0,"pctlogscale":1.6666666666666667} |
| {"value":60.0,"percentile":0.5,"totalcount":529906.0,"pctlogscale":2.0} |
| {"value":61.0,"percentile":0.55,"totalcount":569248.0,"pctlogscale":2.2222222222222223} |
| {"value":62.0,"percentile":0.6,"totalcount":606422.0,"pctlogscale":2.5} |
| {"value":64.0,"percentile":0.65,"totalcount":674614.0,"pctlogscale":2.857142857142857} |
| {"value":65.0,"percentile":0.7,"totalcount":705215.0,"pctlogscale":3.333333333333333} |
| {"value":67.0,"percentile":0.75,"totalcount":760748.0,"pctlogscale":4.0} |
| {"value":68.0,"percentile":0.775,"totalcount":785525.0,"pctlogscale":4.444444444444445} |
| {"value":69.0,"percentile":0.8,"totalcount":808228.0,"pctlogscale":5.000000000000001} |
| {"value":70.0,"percentile":0.825,"totalcount":828819.0,"pctlogscale":5.714285714285713} |
| {"value":72.0,"percentile":0.85,"totalcount":864792.0,"pctlogscale":6.666666666666666} |
| {"value":73.0,"percentile":0.875,"totalcount":880094.0,"pctlogscale":8.0} |
| {"value":74.0,"percentile":0.8875,"totalcount":893834.0,"pctlogscale":8.888888888888886} |
| {"value":75.0,"percentile":0.9,"totalcount":906067.0,"pctlogscale":10.000000000000002} |
| {"value":76.0,"percentile":0.9125,"totalcount":917009.0,"pctlogscale":11.428571428571425} |
| {"value":77.0,"percentile":0.925,"totalcount":926682.0,"pctlogscale":13.333333333333341} |
| {"value":79.0,"percentile":0.9375,"totalcount":942976.0,"pctlogscale":16.0} |
| {"value":80.0,"percentile":0.94375,"totalcount":949604.0,"pctlogscale":17.77777777777777} |
| {"value":81.0,"percentile":0.95,"totalcount":955563.0,"pctlogscale":19.999999999999982} |
| {"value":82.0,"percentile":0.95625,"totalcount":960588.0,"pctlogscale":22.85714285714288} |
| {"value":83.0,"percentile":0.9625,"totalcount":964944.0,"pctlogscale":26.666666666666682} |
| {"value":85.0,"percentile":0.96875,"totalcount":971799.0,"pctlogscale":32.0} |
[...some entries removed...]
| {"value":2271.0,"percentile":0.9999990463256836,"totalcount":1000000.0,"pctlogscale":1048576.0} |
+----------------------------------------------------+
101 rows selected (0.725 seconds)
```

Generate a HDR Histogram as a base64-encoded string.  This can be used with some HDR Histogram tools to visualize the histogram.
```
0: jdbc:hive2://localhost:10000/> select encode_hdr_histogram(value, 2) from sample_data;
+----------------------------------------------------+
|                        _c0                         |
+----------------------------------------------------+
| HISTFAAAAmJ4nC2R4UsTYRzH7/e75x6u65prXcc6jiXHNY61jkOOIUfYGGPEkBGyFyJUw4aJRIgIyZASNRtj7MUKC18NGxYSQSDWi0iTEf0BMnwRvhBZEGJ7KRLVs+z5PvD58OXh9+L36PNPz3EcPuJODv4nnKDrz7X2id1ZeU/339FmnZZf0g8V2vgt7HwX1raF4w1hf0mozgnfWuSgQebr5ONjUt3lDzb5w2W+Osfv7uHuZ/zyChslLP+EnSZUN+DXKlSfQ+MJFB98vVu5uZUpJuveD+f48tal0sXWhe3zR2crXc3TzVNb4h5tk01S41tYxRa8hQpMT6Q8tTuk6bLMiTISTiRIKSEipVSkzMWOMsiyKsuKRgKKPxiUNJ+qBYyArqhayFAUI2IbMcty7ajl9bk9sXiv1duXTAwms1eTiXh/bmgsM5LKZm4MDw6PDWUn87mBoYnC5Hh+FsYLRSg8nJ6FRZiBZbg3AyUowsQLeMakBONLcL8Gb6CIi7iEy1hjWWG2jnUsY4m1C9iGeeZHUMZDaMMq7LGswWv4BOtsYg0W2MDC6CzkZ2BqdCqXHx1JJweuZzNu3PGCdjSshxzb1K2o0d0TNq2IGrItVe9Y1IyEEr3xpOfGIv0xJxJzTC+Uc8PhhJOy04lE2ouahhe3TduNdqdTt1J9kbjrOIZpWxGfrWk6W6uuKXpAUU1D0XQlqhhS0K+ovpCuKFo4oAf8kuJTfGy1kp+JGKCiRGUJZUrlfx8hUbFDwhGCPEEOeaQcIkGBCngGkTVEQIGBXSbkCt5mvYDsAet4iUMZkUoSsrCmk7/yXp0b |
+----------------------------------------------------+
1 row selected (0.858 seconds)
```

This UDAF can be used in 'group by' queries, to generate a set of histograms by attribute.
Create a histogram of the sample_data.values for each value of 'b';
```
select b, hdr_histogram(value, 2) as hist from sample_data group by b;
```

Create base64-encoded histograms that can be visualized using web visualization tools
```
select encode_hdr_histogram(value, 2) from sample_data;
select b, encode_hdr_histogram(value, 2) as hist from sample_data group by b;
```


## Work items

 - [x] Unit tests passing for aggregation
 - [X] manual system tests using HiveQL syntax
 - [ ] simple visualization tool
 - [ ] validation on large-scale data sets

