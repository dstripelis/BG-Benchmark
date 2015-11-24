/**
 * Created by Dstrip on 11/20/15.
 */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks._

object PartitionByDataItem {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("BG Benchmark Validation Phase").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //val read_files = sc.textFile("/Users/Dstrip/Desktop/BGValidationProject/LogRecords/read*.txt")
    //val update_files = sc.textFile("/Users/Dstrip/Desktop/BGValidationProject/LogRecords/update*.txt");
    val read_files = sc.textFile("/Users/Dstrip/Desktop/BGValidationProject/YaazLogRecords/logs-11-15/read*.txt")
    val update_files = sc.textFile("/Users/Dstrip/Desktop/BGValidationProject/YaazLogRecords/logs-11-15/update*.txt");

    //val read_files = sc.textFile("/Users/Dstrip/Desktop/BGBenchmark/MySQL/read*.txt")
    //val update_files = sc.textFile("/Users/Dstrip/Desktop/BGBenchmark/MySQL/update*.txt");


    // e.g. READ,ACCEPTFRND,19,14,3101,68387198211084,68387452718482,10,GetProfile
    case class ReadLog(logType: String, tweakedAttribute: String, seqID: Integer, threadID: Integer, keyname: Integer,
                       startRead: Long, endRead: Long, value: Integer, actionType: String)

    // e.g. UPDATE,ACCEPTFRND,465,221,8423,68494383525897,68494914353028,1,I,AcceptFriend
    case class UpdateLog(logType: String, tweakedAttribute: String, seqID: Integer, threadID: Integer, keyname: Integer,
                         startUpdate: Long, endUpdate: Long, value: Integer, operation: String, actionType: String)

    // Cast Read Logs with the appropriate class
    val readLogs = read_files.map(line => line.split(",")).map(
      logrecord => ReadLog(
        logrecord(0).toString,
        logrecord(1).toString,
        logrecord(2).toInt,
        logrecord(3).toInt,
        logrecord(4).toInt,
        logrecord(5).toLong,
        logrecord(6).toLong,
        logrecord(7).toInt,
        logrecord(8).toString
      )
    )

    // Cast Update Logs with the appropriate class
    val updateLogs = update_files.map(line => line.split(",")).map(
      logrecord => UpdateLog(
        logrecord(0).toString,
        logrecord(1).toString,
        logrecord(2).toInt,
        logrecord(3).toInt,
        logrecord(4).toInt,
        logrecord(5).toLong,
        logrecord(6).toLong,
        logrecord(7).toInt,
        logrecord(8).toString,
        logrecord(9).toString
      )
    )


    val updateIntervalCollection = updateLogs.map({ x =>
      ((x.keyname, x.tweakedAttribute), List(x.startUpdate.toLong, x.endUpdate.toLong, x.operation.toString))
    })
    val readIntervalCollection = readLogs.map({ x =>
      ((x.keyname, x.tweakedAttribute), List(x.startRead.toLong, x.endRead.toLong, x.value.toInt))
    })

    // Group Collections
    val partitionsPerUpdate = updateIntervalCollection.groupByKey()
    val partitionsPerRead = readIntervalCollection.groupByKey()


    // Counter Variable
    var correct_count = sc.accumulator(0)

    val left_joined = partitionsPerRead.leftOuterJoin(partitionsPerUpdate)
    left_joined.foreach{ x=>
      val actionType = x._1._2.toString
      val reads = x._2._1
      val updates = x._2._2

      /** -- Hard Coded Initial State -- **/
      var initial_state = 0
      if (actionType == "ACCEPTFRND"){
        initial_state = 10
      }else if (actionType == "PENDFRND"){
        initial_state = 0
      }


      var dataRange = new java.util.ArrayList[Integer]()

      /** -- If No Updates occured for this partition check against the initial state -- **/
      if (updates.size == 0){
        dataRange.add(initial_state)
        for (readNode <- reads){
          val read_value = readNode(2).asInstanceOf[Int]
          if (dataRange.contains(read_value)){
            correct_count+=1
          }
        }
        dataRange.clear()
      }else{

        // Transform from Some to CompactBuffer
        val updatesIterable = updates.get
        // Sort Iterable by start time of the Updates
        val sortedUpdates = updatesIterable.toList.sortBy(x=> x(0).asInstanceOf[Long])


        /** -- Build The Interval Tree -- **/
        val ITC = new IntervalTreeCustom[Integer]
        var current_value = initial_state
        for (updateNode <- sortedUpdates){
          val update_interval = new Interval1D(updateNode(0).asInstanceOf[Long], updateNode(1).asInstanceOf[Long])
          var update_operation = updateNode(2).toString
          if (update_operation == "I"){
            current_value += 1
          }else if (update_operation == "D"){
            current_value -= 1
          }
          ITC.put(update_interval, current_value)
        }

        val lowestInterval = ITC.getLowestInterval
        val highestInterval = ITC.getHighestInterval

        /** -- Loop Through the Reads and Validate -- **/
        for (readNode <- reads){
          val read_interval = new Interval1D(readNode(0).asInstanceOf[Long], readNode(1).asInstanceOf[Long])
          val read_value = readNode(2).asInstanceOf[Int]

          dataRange = ITC.findDataRange(read_interval)
          if (read_interval.low <= lowestInterval.low){
            dataRange.add(initial_state)
          }
          if (read_interval.low > highestInterval.high){
            dataRange.addAll(ITC.findDataRange(highestInterval))
          }

          /** No Overlap for these Interval; we evaluate against the closest Interval
            * We search for the closest Interval at the Left of the Read Operation
            * */
          if (dataRange.size() == 0){

            //val closest_interval = sortedUpdates.minBy(z=>
            //  if (read_interval.low >= z(1).asInstanceOf[Long]) read_interval.low - z(1).asInstanceOf[Long]
            //  else read_interval.high + z(1).asInstanceOf[Long])
            val (left, right) = sortedUpdates.splitAt(1)
            var closest_interval = left(0)
            var minimum_distance = read_interval.low - closest_interval(1).asInstanceOf[Long]
            breakable {
              for (updateNode <- right) {
                var new_distance = read_interval.low - updateNode(1).asInstanceOf[Long]
                if (updateNode(0).asInstanceOf[Long] > read_interval.high){
                  break
                }else{
                  if (new_distance <= minimum_distance){
                    minimum_distance = new_distance
                    closest_interval = updateNode
                  }
                }
              }
            }

            // Find the Interval Tree Overlaps for the Found Interval
            dataRange = ITC.findDataRange(new Interval1D(closest_interval(0).asInstanceOf[Long], closest_interval(1).asInstanceOf[Long] ))

          }

          if (dataRange.contains(read_value)){
            correct_count += 1
          }

        }//end of Reads

      }//end of IF-Else

    }//end of Partitions Processing


    println("Official total Reads:")
    println(readLogs.count)
    println("Official total Updates:")
    println(updateLogs.count)
    println("Total Correct: " + correct_count.value)
    println("Percentage Correctness: " + correct_count.value.toFloat/readLogs.count)
    println("Percentage Staleness: " + (1-correct_count.value.toFloat/readLogs.count))

  }
}
