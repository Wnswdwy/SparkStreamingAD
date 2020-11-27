package com.nswdwy.app

import java.util.Properties

import com.nswdwy.handler.{BlackListHandler, DateAreaCityAdCountHandler, LastHourAdCountHandler}
import com.nswdwy.util.{MyKafkaUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author yycstart
 * @create 2020-11-25 19:05
 */
object RealTimeApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("RealTimeApp ").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //3.读取数据
    val properties: Properties = PropertiesUtil.load("config.properties")
    val topic: String = properties.getProperty("kafka.topic")

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc)

    //4.将从Kafka读出的数据转换为样例类对象
    val adsLogDStream: DStream[Ads_log] = kafkaDStream.map(record => {

      val value: String = record.value()
      val arr: Array[String] = value.split(" ")

      Ads_log(arr(0).toLong, arr(1), arr(2), arr(3), arr(4))
    })

    //5.需求一：根据MySQL中的黑名单过滤当前数据集
    val filterAdsLogDStream: DStream[Ads_log] = BlackListHandler.filterByBlackList(adsLogDStream)

    //6.需求一：将满足要求的用户写入黑名单
    BlackListHandler.addBlackList(filterAdsLogDStream)
    //测试打印
    filterAdsLogDStream.cache()
    filterAdsLogDStream.count().print()

    //7.需求二：统计每天各大区各个城市广告点击总数并保存至MySQL中
    DateAreaCityAdCountHandler.saveDateAreaCityAdCountToMysql(filterAdsLogDStream)
    //8.需求三：统计最近一小时(2分钟)广告分时点击总数
    val adToHmCountListDStream: DStream[(String, List[(String, Long)])] = LastHourAdCountHandler.getAdHourMintToCount(filterAdsLogDStream)

    //启动任务
    ssc.start()
    ssc.awaitTermination()
  }
}

// 时间 地区 城市 用户id 广告id
case class Ads_log(timestamp: Long, area: String, city: String, userid: String, adid: String)
