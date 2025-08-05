+---------------------+      +------------------------+      +--------------------------+
|  Azure Data Lake    |      |                        |      | Spark DataFrame (Raw)    |
|  (large CSV file)   |----->| Spark Read (`.csv()`)  |----->|                          |
+---------------------+      |                        |      |                          |
                             +------------------------+      +--------------------------+
                                                                         |
                                                                         |
+-----------------------------+                                          | Step 2: MongoDB Lookup
|                             |<-----------------------------------------+ (using UDF)
| MongoDB (`payload` & `ack`) |<-----------------------------------------+
|                             |                                          |
+-----------------------------+                                          |
                                                                         v
+-----------------------------+                                   +--------------------------+
|                             |      +------------------------+   | Spark DataFrame (Enriched)|
| Enriched with flags & status|----->| Split Data into        |   | (PehReprocessingRequired,|
|                             |      | Success & Error Paths  |   | APIDReprocessingRequired,|
+-----------------------------+      +------------------------+   | status, error_details)   |
                                              |                   +--------------------------+
                +-----------------------------+
                |                             |
                |                             |
                v                             v
+------------------------+          +------------------------+
| Spark DataFrame (Error)|          | Spark DataFrame (Success)|
| (status == 'error')    |          | (status == 'success')    |
+------------------------+          +------------------------+
           |                             |
           | Step 3: Write Error File    | Step 4: Prepare Message
           v                             v
+------------------------+          +------------------------+
|  Azure Data Lake       |          |  Event Hubs/Queue      |
|  (error CSV file)      |          |  (Reprocessing Event)  |
+------------------------+          +------------------------+

Breakdown of the Flow
Step 1: Ingestion: The process begins by reading a large CSV file from Azure Data Lake into a Spark DataFrame. The predefined schema ensures efficient parsing.

Step 2: MongoDB Lookup: Each record in the DataFrame triggers a User-Defined Function (UDF). This UDF connects to MongoDB to check for the presence of the message_id in both the payload and acknowledge collections.

Step 3: Enrichment and Splitting: The DataFrame is enriched with new columns (PehReprocessingRequired, APIDReprocessingRequired, processing_status, error_details). The data is then split into two paths: successful records and error records.

Step 4: Error Handling: Records with a processing_status of 'error' are written to a separate error file in Azure Data Lake for later analysis or re-processing.

Step 5: Message Preparation & Output: The successful records are then transformed into a final JSON format, including all the required headers. In a production environment, these messages would be sent to an event queue (like Azure Event Hubs) for downstream consumption.
