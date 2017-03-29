package com.quantifind.kafka.offsetapp

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import com.quantifind.kafka.{OffsetGetter, TopicDetails}
import com.quantifind.kafka.OffsetGetter.KafkaGroupInfo
import com.quantifind.kafka.offsetapp.sqlite.SQLiteOffsetInfoReporter
import com.quantifind.sumac.validation.Required
import com.quantifind.utils.Utils.retry
import com.quantifind.utils.UnfilteredWebApp
import com.twitter.util.Time
import kafka.utils.Logging
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write
import org.json4s.{CustomSerializer, JInt, NoTypeHints}
import unfiltered.filter.Plan
import unfiltered.request.{GET, Path, Seg}
import unfiltered.response.{JsonContent, Ok, ResponseString}

import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.control.NonFatal

class OWArgs extends OffsetGetterArgs with UnfilteredWebApp.Arguments {

  @Required
  var retain: FiniteDuration = _

  @Required
  var refresh: FiniteDuration = _

  var cleanupBackoff : FiniteDuration = new FiniteDuration(6, TimeUnit.HOURS)

  var dbName: String = "offsetapp"

  lazy val db = new OffsetDB(dbName)
}

/**
  * A webapp to track Kafka consumers and their offsets
  * User: pierre
  * Date: 1/23/14
  */
object OffsetGetterWeb extends UnfilteredWebApp[OWArgs] with Logging {

  implicit def funToRunnable(fun: () => Unit) = new Runnable() {
    def run() = fun()
  }

  def htmlRoot: String = "/offsetapp"

  val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(2)

  var dbReporter: SQLiteOffsetInfoReporter = null

  def retryTask[T](fn: => T) {
    try {
      retry(3) {
        fn
      }
    } catch {
      case NonFatal(e) =>
        error("Failed to run scheduled task", e)
    }
  }

  def reportOffsets(args: OWArgs) : Unit = {
    val groups = getKafkaGroups(args)
    groups.foreach {
      g =>
        try {
          val inf = getKafkaGroupInfo(g, args).offsets.toIndexedSeq
          if (dbReporter != null) {
            debug(s"reporting ${inf.size}")
            retryTask { dbReporter.report(inf) }
          }
        } catch {
          case NonFatal(t) =>
            error(s"could not report offsets for group $g", t)
        }
    }
  }

  def cleanupOldData(args: OWArgs): Unit = {
    retryTask { dbReporter.cleanupOldData() }
  }

  def schedule(args: OWArgs) {
    scheduler.scheduleAtFixedRate(() => {reportOffsets(args)}, 0, args.refresh.toMillis, TimeUnit.MILLISECONDS)
    scheduler.scheduleAtFixedRate(() => {cleanupOldData(args)}, 0, args.cleanupBackoff.toMillis, TimeUnit.MILLISECONDS)
  }

  // -------------------------------------------------------------------------------------------------------------------
  // proxy operation to the real implementation of OffsetGetter
  def withOG[T](args: OWArgs)(f: OffsetGetter => T): T = {
    var og: OffsetGetter = null
    try {
      val offsetGetter: OffsetGetter = OffsetGetter.getInstance(args)
      f(offsetGetter)
    } finally {
      // ignored
    }
  }

  def getKafkaGroupInfo(group: String, args: OWArgs): KafkaGroupInfo = withOG(args) {
    _.getKafkaGroupInfo(group)
  }

  def getKafkaGroups(args: OWArgs) = withOG(args) {
    _.getKafkaGroups
  }

  def getKafkaActiveTopics(args: OWArgs) = withOG(args) {
    _.getKafkaActiveTopics
  }

  def getKafkaTopics(args: OWArgs) = withOG(args) {
    _.getKafkaTopics
  }

  def getKafkaTopicDetails(topic: String, args: OWArgs) = withOG(args) {
    _.getKafkaTopicDetails(topic)
  }

  def getKafkaTopicConsumersDetails(topic: String, args: OWArgs) = withOG(args) {
    _.getKafkaTopicAndConsumersDetails(topic)
  }

  def getKafkaClusterViz(args: OWArgs) = withOG(args) {
    _.getKafkaClusterViz
  }

  // -------------------------------------------------------------------------------------------------------------------

  /**
    * Custom serializer Scala Time <-> epoch time
    */
  class TimeSerializer extends CustomSerializer[Time](
    format => ( {
      case JInt(s) => Time.fromMilliseconds(s.toLong)
    }, {
      case x: Time => JInt(x.inMilliseconds)
    })
  )

  override def setup(args: OWArgs): Plan = new Plan {
    args.db.maybeCreate()
    dbReporter = new SQLiteOffsetInfoReporter(args.db, args)

    // launch automatic Kafka state tracking
    schedule(args)

    // converting Scala datatypes to JSON format
    implicit val formats = Serialization.formats(NoTypeHints) + new TimeSerializer
    // define web application mapping
    def intent = unfiltered.kit.GZip {
      case GET(Path(Seg("group" :: Nil))) =>
        val groups = getKafkaGroups(args)
        JsonContent ~> ResponseString(write(groups))

      case GET(Path(Seg("group" :: group :: Nil))) =>
        val info = getKafkaGroupInfo(group, args)
        JsonContent ~> ResponseString(write(info)) ~> Ok

      case GET(Path(Seg("group" :: group :: topic :: Nil))) =>
        val offsets = args.db.offsetHistory(group, topic)
        JsonContent ~> ResponseString(write(offsets)) ~> Ok

      case GET(Path(Seg("topiclist" :: Nil))) =>
        val topics = getKafkaTopics(args)
        JsonContent ~> ResponseString(write(topics))

      case GET(Path(Seg("clusterlist" :: Nil))) =>
        val node = getKafkaClusterViz(args)
        JsonContent ~> ResponseString(write(node))

      case GET(Path(Seg("topicdetails" :: topic :: Nil))) =>
        val topicDetails = getKafkaTopicDetails(topic, args)
        JsonContent ~> ResponseString(write(topicDetails))

      case GET(Path(Seg("topic" :: topic :: "consumer" :: Nil))) =>
        val topicAndConsumersDetails = getKafkaTopicConsumersDetails(topic, args)
        JsonContent ~> ResponseString(write(topicAndConsumersDetails))

      case GET(Path(Seg("activetopics" :: Nil))) =>
        val node = getKafkaActiveTopics(args)
        JsonContent ~> ResponseString(write(node))
    }
  }

  override def afterStop() {
    scheduler.shutdown()
  }

}
