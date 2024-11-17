The operation is Spark session created

The operation is load data

The truncated output is: 

|    | date     | location           | city           | state   |     lat |      lng |
|---:|:---------|:-------------------|:---------------|:--------|--------:|---------:|
|  0 | 9/1/2016 | Wilmington, OH     | Wilmington     | OH      | 39.4453 | -83.8285 |
|  1 | 9/3/2016 | Detroit, MI        | Detroit        | MI      | 42.3314 | -83.0458 |
|  2 | 9/6/2016 | Virginia Beach, VA | Virginia Beach | VA      | 36.8529 | -75.978  |
|  3 | 9/6/2016 | Greenville, NC     | Greenville     | NC      | 35.6127 | -77.3664 |
|  4 | 9/7/2016 | New York, NY       | New York       | NY      | 40.7128 | -74.0059 |

The operation is Query data

The truncated output is: 

|    | date      | location           | city           | state   |     lat |      lng |
|---:|:----------|:-------------------|:---------------|:--------|--------:|---------:|
|  0 | 9/3/2016  | Detroit, MI        | Detroit        | MI      | 42.3314 | -83.0458 |
|  1 | 9/7/2016  | New York, NY       | New York       | NY      | 40.7128 | -74.0059 |
|  2 | 9/13/2016 | Des Moines, IA     | Des Moines     | IA      | 41.6005 | -93.6091 |
|  3 | 9/14/2016 | Canton, OH         | Canton         | OH      | 40.7989 | -81.3784 |
|  4 | 9/15/2016 | New York, NY       | New York       | NY      | 40.7128 | -74.0059 |
|  5 | 9/15/2016 | Laconia, NH        | Laconia        | NH      | 43.5279 | -71.4704 |
|  6 | 9/21/2016 | Toledo, OH         | Toledo         | OH      | 41.6639 | -83.5552 |
|  7 | 9/26/2016 | Hemstead, NY       | Hemstead       | NY      | 40.7062 | -73.6187 |
|  8 | 9/28/2016 | Council Bluffs, IA | Council Bluffs | IA      | 41.2619 | -95.8608 |
|  9 | 9/28/2016 | Waukesha, WI       | Waukesha       | WI      | 43.0117 | -88.2315 |

The operation is transform data

The truncated output is: 

|    | date      | location           | city           | state   |     lat |      lng | Region   |
|---:|:----------|:-------------------|:---------------|:--------|--------:|---------:|:---------|
|  0 | 9/1/2016  | Wilmington, OH     | Wilmington     | OH      | 39.4453 | -83.8285 | Central  |
|  1 | 9/3/2016  | Detroit, MI        | Detroit        | MI      | 42.3314 | -83.0458 | Central  |
|  2 | 9/6/2016  | Virginia Beach, VA | Virginia Beach | VA      | 36.8529 | -75.978  | East     |
|  3 | 9/6/2016  | Greenville, NC     | Greenville     | NC      | 35.6127 | -77.3664 | East     |
|  4 | 9/7/2016  | New York, NY       | New York       | NY      | 40.7128 | -74.0059 | East     |
|  5 | 9/9/2016  | NW, Washington, DC | Washington     | DC      | 38.9072 | -77.0369 | East     |
|  6 | 9/9/2016  | Pensacola, FL      | Pensacola      | FL      | 30.4213 | -87.2169 | Central  |
|  7 | 9/12/2016 | Baltimore, MD      | Baltimore      | MD      | 39.2904 | -76.6122 | East     |
|  8 | 9/12/2016 | Asheville, NC      | Asheville      | NC      | 35.5951 | -82.5515 | Central  |
|  9 | 9/13/2016 | Des Moines, IA     | Des Moines     | IA      | 41.6005 | -93.6091 | Central  |

The operation is Spark session stopped.

