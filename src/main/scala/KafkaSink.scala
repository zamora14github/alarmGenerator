import java.sql.{Connection, DriverManager}

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.spark.sql.ForeachWriter


/** Custom sink to send the results to MySQL
  *
  *
  */

class KafkaSink(pTopic:String, servers:String) extends org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row] {

  import shaded.parquet.org.slf4j.LoggerFactory

  private val logger = LoggerFactory.getLogger(classOf[Nothing])
var producer: KafkaProducer[String, String] = _


  /** Initializes the process
    *
    * @param partitionId
    * @param version
    * @return
    */
    def open(partitionId: Long, version: Long): Boolean = {

      // Create Kafka Producer for the alarms
      producer = new Producer().getProducer
      true
    }



  /** Executes the process
    *
    * @param pValue a list with the plc parameters and values
    */
    def process(pValue: org.apache.spark.sql.Row): Unit = {

      // Producer topics routing
      val topic = pTopic
      val key = Parser.parseAlarmKey("alarm") // Parsed key
      val alarmMessage = Parser.parseAlarm(pValue)
      val value = Parser.parseAlarmValue(pValue,alarmMessage) // Parsed value

      if(alarmMessage != ""){
        // Create Kafka Producer
        val record = new ProducerRecord[String, String](topic, key, value)


        // Send Data to Kafka - asynchronous
        producer.send(record, new Callback() {
          override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = { // executes every time a record is successfully sent or an exception is thrown
            if (e == null) { // the record was successfully sent
              logger.info("Received new metadata. \n" + "Topic:" + recordMetadata.topic + "\n" + "Partition: " + recordMetadata.partition + "\n" + "Offset: " + recordMetadata.offset + "\n" + "Timestamp: " + recordMetadata.timestamp)
              print("Received and sent new record. \n" + "Topic:" + recordMetadata.topic + "\n" + "Partition: " + recordMetadata.partition + "\n" + "Offset: " + recordMetadata.offset + "\n" + "Timestamp: " + recordMetadata.timestamp)
            }
            else logger.error("Error while producing", e)
          }
        })


      }
    }



  /** Ends the process
    *
    * @param errorOrNull a message error if there is an error or null if there is not an error
    */
    def close(errorOrNull: Throwable): Unit = {
      producer.close()

    }
  }