import java.sql.Timestamp
import java.util.Calendar

/** Provides insert (row-based) methods
  *
  */
object Parser {

  /** Creates an insert query
   *
   * @param value a list with the source kafka topic fields
   * @return an insert query
   */
  def parseAlarm(value: org.apache.spark.sql.Row): String = {
    val model = value(1)

    val valueLength9 = getStringLength(value(9).toString)
    val valueI1 = value(9).toString.substring(13,valueLength9)

    val splitted = valueI1.split(",").map(_.trim)
    var message:String = ""

    if (model.equals("s1500") | model.equals("s300")) {
        if(splitted(7).equals("true"))
          message += "Valve 0 damaged. "
        if(splitted(6).equals("true"))
          message += "Valve 1 damaged. "
        if(splitted(5).equals("true"))
          message += "Valve 2 damaged. "
        if(splitted(4).equals("true"))
          message += "Valve 3 damaged. "
        if(splitted(3).equals("true"))
          message += "Valve 4 damaged. "
        if(splitted(2).equals("true"))
          message += "Valve 5 damaged. "
        if(splitted(1).equals("true"))
          message += "Valve 6 damaged. "
        if(splitted(0).equals("true"))
          message += "Valve 7 damaged. "

    }


    message
  }


  /** Calculates the length - 1 of a String
   *
   * @param pString a String
   * @return the length of the String minus 1
   */
  def getStringLength(pString: String): Int ={
    pString.length - 1
  }


  /** Parse the alarm key
   *
   * @param kValue  String
   * @return a parsed String
   */
   def parseAlarmKey(kValue: String): String = {
    val parsedString = kValue.replace("alarm", "{\"protocol\":\"alarm\"}")
    parsedString
  }

  //
  /** Parse messages by adding additional fields (alarm_address, alarm_timestamp, alarm_message)
   *
   * @param vValue a org.apache.spark.sql.Row
   * @param pAlarmMessage  String
   * @return the length of the String minus 1
   */
  def parseAlarmValue(vValue: org.apache.spark.sql.Row, pAlarmMessage:String): String = {
    val alarmAddress = vValue(0)
    val alarmTimestamp = vValue(3)
    val alarmMessage = pAlarmMessage
    val alarmParsed = "{\"alarm_address\":\"" + alarmAddress + "\",\"alarm_timestamp\":\"" + alarmTimestamp +
      "\",\"alarm_message\":\"" + alarmMessage + "\"}"
    alarmParsed
  }



}
