# alarmGenerator
This Spark application is in charge of reading the Kafka topics and searching within the s7_readings topic if one or more of the corresponding PLC inputs I1.0 - I1.7 are activated and in that case it generates an alarm message indicating that there are damaged valves. That message is sent to Kafka so that later the application "sparkTransformer" can read this data.
