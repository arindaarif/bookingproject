Reporting Pipeline
===================

As informed in Technical Test (Business Intelligence Data Engineer) instruction, I designed a reporting pipeline that contains booking information across GO-JEK products. The pipeline is created to produce hourly window of report with 5 minutes period. Based on json file as an input, the report will contain Total Booking information based on Service Area Name, Booking Status, and Payment Channel. 

<img src="Data Pipeline Flow.jpg" alt="Data Pipeline Flow" />

The data pipeline flow above shows input file, output file, and every PTransform & type of PCollection between them. Here are the explanation & data model for each step of the pipeline.

1 - Read File
---------
**Explanation**<br/>
First, I applied a text file Read transform (TextIO.read()) to the pipeline object I created. This PTransform will generate a PCollection output that represent lines from a json file named “Booking.json”, located in ‘/files/input’ directory path.

**Data Model**<br/>
String formatted in Json, for example:<br/>
  {"order_number":"AP-1","service_type":"GO_RIDE","driver_id":"driver-123","customer_id":"customer-456","service_area_name":"JAKARTA","payment_type":"GO_PAY","status":"COMPLETED","event_timestamp":"2018-03-29T10:00:08.000Z"}

2- ParDo named “ParseBooking”
--------------------------
**Explanation**<br/>
This ParDo will invoke a class that transform PCollection of lines (in json format) into objects of BookingData using Jackson. I also give timestamp value to each element of this collection based on the ‘event_timestamp’ field from each object. Since the event_timestamp fields parsed in String, it needs to be formatted using java.text.SimpleDateFormat. The output of this transform is PCollection of BookingData.

**Data Model**<br/>
I use BookingData class as a model. This class consisted of these fields (all in string): service_area_name, payment_type, status, order_number, service_type, driver_id, customer_id, event_timestamp.

3 - Windowing
--------------
**Explanation**<br/>
In this part, I applied a windowing configuration that sets hourly sliding windows with 5 minutes period.

**Data Model**<br/>
This PTransform did not generate any output differed from the input.

4 -	Mapping
------------
**Explanation**<br/>
This ParDo will invoke a class that transform PCollection of BookingData into pairs of key  and value. Key is a String consisted of ‘service_area_name’, ‘payment_type’, and ‘status’. Value is used to count elements and using data type of Integer.

**Data Model**<br/>
This PTransform generate outputs in PCollection of KV(String, Integer). The data type of Key is a string with this format: service_area_name+’, ‘+payment_type+’, ‘+status. Meanwhile, the Value is set to 1 so the next transforms will be able to easily sum the value to get the total booking. For example:
KV(“JAKARTA, GO_PAY, COMPLETE”,1)


5 -	GroupByKey
---------------
**Explanation**<br/>
This transform will aggregate the collections with same keys, so we finally have distinct collections with unique keys and iterable values.

**Data Model**<br/>
Since the previous PCollection of pairs has been grouped by key, this PTransform will generate outputs in PCollection of KV(String, Iterable<Integer>). For example, if in 1 window we have 5 elements with key “JAKARTA, GO_PAY, COMPLETE”, then the output will be:
KV(“JAKARTA, GO_PAY, COMPLETE”, (1,1,1,1,1))

6 -	SumUpValuesByKey
--------------------
**Explanation**<br/>
This ParDo will invoke a class that sums up iterable values for each element to be used as total booking for each element. The output of this transform is PCollection of string that reports ‘service_area_name’, ‘payment_type’, ‘status’, and total booking count for those criteria in each window. 

**Data Model**<br/>
In this PTransform, I concat the key and the summed up value into a string for each element. For example, if in 1 window we have 5 elements with key “JAKARTA, GO_PAY, COMPLETE”, then the output will be:
“JAKARTA, GO_PAY, COMPLETE, 5”

7 -	Write to csv file
---------------------
**Explanation**<br/>
The last transform is to write PCollection of Strings into csv files using TextIO.write(). The name of the files will be BookingReport with the information of start and end time of the window. These files will be stored in ‘/files/output’ directory path.

**Data Model**<br/>
Since in this test I write to files using TextIO, the input for this PTransform has to be Strings.

-----------------------

The source code of dummy application to apply this pipeline is also can be found along with this file. The application was made using Java 8, with Apache beam 2.4.0 as the pipeline API and Direct-Runner as the runner. I also created unit testing for the transformation classes using JUnit.

The reason I am using Apache beam instead of other pipeline API is because as I researched, Apache Beam supports **multiple runner backends** and provides a higher portability and functionality. For example, I am able to apply GroupByKey transform into my pipeline using Beam model, but not in Spark (since the mentioned transform is not fully supported). 

**However, there are limitations in this project.**
- The final state in this test is CSV files for every windows created. I would prefer to load those unbounded data into Google BigQuery, where it can be analyzed effectively to provide business intelligence. 
- The current sliding window configuration needs to be considered further, especially considering the data latency and quality. 

