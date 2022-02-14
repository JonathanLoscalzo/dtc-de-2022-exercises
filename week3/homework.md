
## Homework
[Form](https://forms.gle/ytzVYUh2RptgkvF79)  
We will use all the knowledge learned in this week. Please answer your questions via form above.  
**Deadline** for the homework is 14th Feb 2022 17:00 CET.

### Question 1: 
**What is count for fhv vehicles data for year 2019**  
Can load the data for cloud storage and run a count(*)


```
select sum(count) from (
	select count(1) from fhv_tripdata_2019_01
	union select count(1) from fhv_tripdata_2019_02
	union select count(1) from fhv_tripdata_2019_03
	union select count(1) from fhv_tripdata_2019_04
	union select count(1) from fhv_tripdata_2019_05
	union select count(1) from fhv_tripdata_2019_06
	union select count(1) from fhv_tripdata_2019_07
	union select count(1) from fhv_tripdata_2019_08
	union select count(1) from fhv_tripdata_2019_09
	union select count(1) from fhv_tripdata_2019_10
	union select count(1) from fhv_tripdata_2019_11
	union select count(1) from fhv_tripdata_2019_12
) as T

#38554089
```

### Question 2: 
**How many distinct dispatching_base_num we have in fhv for 2019**  
Can run a distinct query on the table from question 1

```
select distinct count(dispatching_base_num) from (
	select distinct dispatching_base_num from fhv_tripdata_2019_01
	union select distinct dispatching_base_num from fhv_tripdata_2019_02
	union select distinct dispatching_base_num from fhv_tripdata_2019_03
	union select distinct dispatching_base_num from fhv_tripdata_2019_04
	union select distinct dispatching_base_num from fhv_tripdata_2019_05
	union select distinct dispatching_base_num from fhv_tripdata_2019_06
	union select distinct dispatching_base_num from fhv_tripdata_2019_07
	union select distinct dispatching_base_num from fhv_tripdata_2019_08
	union select distinct dispatching_base_num from fhv_tripdata_2019_09
	union select distinct dispatching_base_num from fhv_tripdata_2019_10
	union select distinct dispatching_base_num from fhv_tripdata_2019_11
	union select distinct dispatching_base_num from fhv_tripdata_2019_12
) as T

# 791
```

### Question 3: 
**Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num**  
Review partitioning and clustering video.   
We need to think what will be the most optimal strategy to improve query 
performance and reduce cost.

` Partition by dropoff_datetime and cluster by dispatching_base_number `

### Question 4: 
**What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279**  
Create a table with optimized clustering and partitioning, and run a 
count(*). Estimated data processed can be found in top right corner and
actual data processed can be found after the query is executed.

`I don't know`

### Question 5: 
**What will be the best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag**  
Review partitioning and clustering video. 
Clustering cannot be created on all data types.

```
SR_Flags is always empty

Looking for documentation: https://cloud.google.com/bigquery/docs/clustered-tables

Clustering: 
- Your queries commonly use filters or aggregation against multiple particular columns.
- The cardinality of the number of values in a column or group of columns is large.

Prefer clustering over partitioning under the following circumstances:

- Partitioning results in a small amount of data per partition (approximately less than 1 GB).
- Partitioning results in a large number of partitions beyond the limits on partitioned tables.
- Partitioning results in your mutation operations modifying most partitions in the table frequently (for example, every few minutes).


We have 791 dispatch_base_number, so we could use partition in that case. 

But dispatch_base_number doesn't have granularity or hieararchical values (like dates), so I think it could be more interesting to use it as a cluster.
But, depending on the case, we could use it as partition, although it could be more convenient use date to partitions ( due to the hiearchical naturally quality).

I didn't test it out at BQ, so the "prefer" section is just information (to me), not a decision.
At a glance, I will use it with clustering, but if we need more information to be "sliced", I will use it partition. 
I our case, I think that


If I execute the following query: 


select sum(count), dispatching_base_num from (
select distinct count(1), dispatching_base_num from fhv_tripdata_2019_01 group by dispatching_base_num
	union select count(1), dispatching_base_num from fhv_tripdata_2019_02 group by dispatching_base_num
	union select count(1), dispatching_base_num from fhv_tripdata_2019_03 group by dispatching_base_num
	union select count(1), dispatching_base_num from fhv_tripdata_2019_04 group by dispatching_base_num
	union select count(1), dispatching_base_num from fhv_tripdata_2019_05 group by dispatching_base_num
	union select count(1), dispatching_base_num from fhv_tripdata_2019_06 group by dispatching_base_num
	union select count(1), dispatching_base_num from fhv_tripdata_2019_07 group by dispatching_base_num
	union select count(1), dispatching_base_num from fhv_tripdata_2019_08 group by dispatching_base_num
	union select count(1), dispatching_base_num from fhv_tripdata_2019_09 group by dispatching_base_num
	union select count(1), dispatching_base_num from fhv_tripdata_2019_10 group by dispatching_base_num
	union select count(1), dispatching_base_num from fhv_tripdata_2019_11 group by dispatching_base_num
	union select count(1), dispatching_base_num from fhv_tripdata_2019_12 group by dispatching_base_num
) as T
group by dispatching_base_num
order by sum desc

The top ten dispatching_base_number has these numbers: 
SUM     DISPATCHING_BASE_NUMBER
3889987	B02510
1419525	B02764
904619	B02765
855630	B02875
845866	B02872
838280	B02800
655675	B02869
541313	B02914
539097	B02887
531131	B02682

Then, I'll decide to choose clustering, because other dispatching numbers haven't have a huge amount of data.
```

### Question 6: 
**What improvements can be seen by partitioning and clustering for data size less than 1 GB**  
Partitioning and clustering also creates extra metadata.  
Before query execution this metadata needs to be processed.

```
from: https://cloud.google.com/bigquery/docs/clustered-tables

> Prefer clustering over partitioning under the following circumstances:
>    Partitioning results in a small amount of data per partition (approximately less than 1 GB).

```

### (Not required) Question 7: 
**In which format does BigQuery save data**  
Review big query internals video.

```
I know the answer is columnar, but I don't know if (at internals) is a cassandra or presto solution, or a mix. 

```


