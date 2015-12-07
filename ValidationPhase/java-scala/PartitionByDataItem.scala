/**
 * Created by Dstrip on 11/20/15.
 */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.control.Breaks._
import scala.compat._

object PartitionByDataItem {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("BG Benchmark Validation Phase").setMaster("local[4]")
    val sc = new SparkContext(conf)

    /** -- get the directory of the log files from the command line args -- **/
    /** -- check if it ends with '/' or else add it -- **/
    var directory = args(0).toString
    if (!(directory.takeRight(1) == "/")){
      directory += "/"
    }

    val read_files = sc.textFile(directory + "read*.txt")
    val update_files = sc.textFile(directory + "update*.txt");

    // Start time of program execution
    val START_TIME = Platform.currentTime;

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


    // Shared Counter Variable
    var correct_count = sc.accumulator(0)
    var stale_count = sc.accumulator(0)
    var total_updates = sc.accumulator(0)
    var total_reads = sc.accumulator(0)

    /**
    *   FOR ALL THE FOLLOWING COMPUTATIONS REMEMBER THAT AN INTERVAL IS IN THE FORM:
    *   Interval: [low, high],
    *   where low is the start of the interval and high is the end of the interval
    */

    val left_joined = partitionsPerRead.leftOuterJoin(partitionsPerUpdate)
    left_joined.foreach{ x =>
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

      /** Here we store all the possible range values **/
      var dataRange = new java.util.ArrayList[Integer]()

      /** -- If No Updates occurred for this partition we check against the initial state -- **/
      if (updates.size == 0){
        dataRange.add(initial_state)
        for (readNode <- reads){
          total_reads+=1
          val read_value = readNode(2).asInstanceOf[Int]
          if (dataRange.contains(read_value)){
            correct_count+=1
          }
        }
        dataRange.clear()

      }else{

        // Transform from Some to CompactBuffer
        val updatesIterable = updates.get
        // Sort Updates Iterable by the start time of the Updates
        val sortedUpdates = updatesIterable.toList.sortBy(x=> x(0).asInstanceOf[Long])


        /** -- Build The Interval Tree -- **/
        val ITC = new IntervalTreeCustom[Integer]
        var current_value = initial_state
        for (updateNode <- sortedUpdates){

          total_updates += 1

          val update_interval = new Interval1D(updateNode(0).asInstanceOf[Long], updateNode(1).asInstanceOf[Long])
          val update_operation = updateNode(2).toString
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
          total_reads+=1
          val read_interval = new Interval1D(readNode(0).asInstanceOf[Long], readNode(1).asInstanceOf[Long])
          val read_value = readNode(2).asInstanceOf[Int]

          /** Find the Overlapping Value Range of the Read **/
          dataRange = ITC.findDataRange(read_interval)

          /** If the Read Interval low is smaller than Interval Tree's lowest low, then add the initial state to the value range **/
          if (read_interval.low <= lowestInterval.low){
            dataRange.add(initial_state)
          }

          /** If the Read Operation is after all of the Updates then find the overlaps of the last interval of the tree **/
          if (read_interval.low > highestInterval.high){
            dataRange.addAll(ITC.findDataRange(highestInterval))
          }

          /**
          *   We search for the closest Interval on the left of the Read operation
          *   The closest interval is the one with the closest high(end time)
          *   Since the updates are sorted by their start time, when we see the first update interval
          *   for which the high is greater or equal to the Read's low (means there is an overlap) we stop
          */
          var closest_interval_low : Option[Long] = None
          var closest_interval_high : Option[Long] = None
          var exists = false
          breakable {
            for (updateNode <- sortedUpdates) {
              if (updateNode(1).asInstanceOf[Long] <= read_interval.low){
                closest_interval_low = Some(updateNode(0).asInstanceOf[Long])
                closest_interval_high = Some(updateNode(1).asInstanceOf[Long])
                exists = true
              }else{
                break()
              }
            }
          }
          if (exists){
            dataRange.addAll(ITC.findDataRange(new Interval1D(closest_interval_low.get, closest_interval_high.get)))
            //val value = ITC.get(new Interval1D(closest_interval_low.get, closest_interval_high.get))
            //dataRange.add(value.toInt-1)
            //dataRange.add(value.toInt+1)
          }else{
            dataRange.add(initial_state)
          }

          if (dataRange.contains(read_value)){
            correct_count += 1
          }else{
            stale_count += 1
//            println()
//            println("*****************")
//            println("Read Value: " + read_value);
//            println("Reads:")
//            println(read_interval)
//            println("Closest Interval:")
//            println(closest_interval_low, closest_interval_high)
//            if (exists){
//              println("Closest Interval Value: " + ITC.get(new Interval1D(closest_interval_low.get, closest_interval_high.get)))
//            }
//            println("Updates Value: " + dataRange);
//            println("Updates:")
//            println(sortedUpdates)
//            println("*****************")
//            println()
          }

        }//end of Reads

      }//end of IF-Else

    }//end of Partitions Processing

    val END_TIME = Platform.currentTime;

    println("Total Execution TIme:")
    println(END_TIME-START_TIME + " milliseconds")

    println("Official total Reads:")
    println(total_reads.value)
    println("Official total Updates:")
    println(update_files.count)
    println("Processed total Updates:")
    println(total_updates.value)
    println("Total Correct Reads: " + correct_count.value)
    println("Total Stale Reads: " + stale_count.value)
    println("Percentage Correctness: " + correct_count.value.toFloat/total_reads.value)
    println("Percentage Staleness: " + (stale_count.value.toFloat/total_reads.value))

  }
}
