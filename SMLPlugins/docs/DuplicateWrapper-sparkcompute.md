# Duplicate Marker Spark Compute

Description
-----------

This is Plugin for CDAP which used the duplicate marker function. The duplicate marker function is used to mark
duplicate records given a set of dats. The way the function does this is by windowing over a dataframe, the data
is then ordered and marked based on this order, a new column is then created which markers the record with either
 a 1 or a 0.

|Markers | Description |
| --- | --- |
|1 | not  duplicate. |
|0 | duplicate. |


Use Case
--------

The plugin should be used when you need to find out if there are duplicate records.

Properties
----------

| Parameter | Description | Default |
| --- | --- | --- |
| dataframe | Input Dataframe . | N/A |
| partCol | This is the column name that will be used for partitioning columns  .| N/A |
| ordCol | This is the column name that will be used for ordering columns. | N/A |
| new_col | This is what you want to call the added column that holds all the marked records. | N/A |

Example
-------

An example of how this plugin works, is by taking the following dataset:

| id | num| order |
| --- | --- | --- |
| 1 | 4 | 1 |
| 1 | 4 | 2 |
| 1 | 5 | 3 |
| 2 | 4 | 1 |

Becomes the following dataset after the duplicate marker function is used:

| id | num| order | marker |
| --- | --- | --- | --- |
| 1 | 4 | 1 | 1 |
| 1 | 4 | 2 | 0 |
| 1 | 5 | 3 | 1 |
| 2 | 4 | 1 | 1 |

Using:

   {
        "name": "DuplicateWrapper",
        "type": "spark-compute",
        "properties": {
            "Dataframe": "input dataframe"
            "partCol": "partition columns",
            "ordCol": "order columns",
            "new_col": "marker",
        }
    }
