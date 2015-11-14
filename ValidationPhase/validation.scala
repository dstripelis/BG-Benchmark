package spark.bg.validate;

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._
import scalaz.Scalaz._


object Main {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("BG Benchmark Validation Phase").setMaster("local[8]")
    val sc = new SparkContext(conf)

    val read_files = sc.textFile("/Users/Dstrip/Desktop/BGValidationProject/LogRecords/read*.txt")
    val update_files = sc.textFile("/Users/Dstrip/Desktop/BGValidationProject/LogRecords/update*.txt");

    // e.g. READ,ACCEPTFRND,19,14,3101,68387198211084,68387452718482,10,GetProfile
    case class ReadLog(logType:String, tweakedAttribute:String, seqID:Integer, threadID:Integer, keyname:Integer,
                       startRead:Long, endRead:Long, value:Integer, actionType:String )

    // e.g. UPDATE,ACCEPTFRND,465,221,8423,68494383525897,68494914353028,1,I,AcceptFriend
    case class UpdateLog( logType:String, tweakedAttribute:String, seqID:Integer, threadID:Integer, keyname:Integer,
                          startUpdate:Long, endUpdate:Long, value:Integer, operation:String, actionType:String )

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

    // Create a collection of (userid, Map[interval.low, interval.high] -> List(value_of_interval))
    val updateIntervalCollection = updateLogs.map({ x=> ((x.keyname, x.tweakedAttribute), Map[(Long,Long),List[String]]((x.startUpdate,x.endUpdate)->List(x.value.toString))) })
    val readIntervalCollection = readLogs.map({ x=> ((x.keyname,x.tweakedAttribute) , Map[(Long,Long),List[String]]((x.startRead,x.endRead)->List(x.value.toString)) ) })

    /** Reduce the interval collections by (user_id, data_item) and group together inside the Map
      * the values of same intervals -- (interval.low, interval.high)
      * We reduce for both collections **NOT GROUP_BY** to improve performance
     */
    val partitionsPerUpdate = updateIntervalCollection.reduceByKey((x,y)=> x |+| y)
    val partitionsPerRead = readIntervalCollection.reduceByKey((x,y)=> x |+| y)


    val stalenessCount = sc.accumulator(0)

    partitionsPerUpdate.cogroup(partitionsPerRead).foreach{ x=> {
        // Take the Updates and build the Interval Tree
        var updates = x._2._1
        var updateNodes : Map[(Long,Long),List[String]] = Map()
        var ITC = new IntervalTreeCustom[List[String]]()

        if (updates.size > 0){
          updateNodes = updates.toSeq.apply(0)
          for (update_node <- updateNodes){
            ITC.put(new Interval1D(update_node._1._1,update_node._1._2), update_node._2)
          }
        }

        var reads = x._2._2
        if (reads.size > 0){
          var readNodes = reads.toSeq.apply(0)
          for (read_node <- readNodes){
            var read_interval = new Interval1D(read_node._1._1, read_node._1._2)
            var read_value = read_node._2
            var drange : Set[String] = Set()

            /** Evaluation cases, with the sequence they are executed:
              * Case1 (ITC.height==0): No updates happened check against initial state
              * Case4 (Result==0, R.low > Highest.high): Read operation after all updates
              * Case2,3 (Result==0, R.low < Lowest.low): We have to include the initial state
              * Case5 (Result==0): Read operation does not overlap with any of its updates(occurs in the middle):
             */
            if (ITC.height() == 0){
              // Check against Initial State
              drange+=10.toString
            }else {
              drange = ITC.findDataRange(read_interval).toSet.flatten
              if (drange.size == 0 && read_interval.low > ITC.getHighestInterval.high) {
                // Check against final Interval
                drange = ITC.findDataRange(ITC.getHighestInterval).toSet.flatten

              } else if (drange.size==0 && read_interval.low < ITC.getLowestInterval.low) {
                // Check or Push Initial State
                drange+=10.toString
              }
            }
            // Case5: Read operation does not overlap with any of its updates(occurs in the middle):
            // <---U1---> <---R1---> <---U2--->
            // find the closest minimum interval to the Read
            if (drange.size == 0 && updateNodes.size>0){
              var closestInterval = updateNodes.minBy(x=> x._1._2-read_interval.low)
              drange = ITC.findDataRange(new Interval1D(closestInterval._1._1, closestInterval._1._2)).toSet.flatten
            }

            //check if the result exists inside the returned Set
            // Read_Value is a List -- we need the first Element ** only one value per read operation **
            if ( !drange.contains(read_value(0)) ){
              stalenessCount += 1
            }


          }
        }

    }}


    println("Total: " + readLogs.count)
    println("Total Stale Reads: " + stalenessCount.value)
    println("Staleness: " + stalenessCount.value.toFloat/readLogs.count)
    println("Correctness: " + (1-(stalenessCount.value.toFloat/readLogs.count)))

  }
}
