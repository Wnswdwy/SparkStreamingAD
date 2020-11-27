package com.nswdwy.handler

import java.sql.Date
import java.text.SimpleDateFormat

import com.nswdwy.app.Ads_log
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream

/**
 * @author yycstart
 * @create 2020-11-25 22:17
 */
object LastHourAdCountHandler {
  //时间格式化对象
  val sdf: SimpleDateFormat = new SimpleDateFormat("HH-mm")

  def getAdHourMintToCount(filterAdsLogDStream: DStream[Ads_log]): DStream[(String, List[(String, Long)])] = {
      // 1. 开窗 =》时间间隔为1小时window

    val windowAdsLOgDStreeam: DStream[Ads_log] = filterAdsLogDStream.window(Minutes(2))
    //2 转换数据结构 ads_log => (adid,hm),1L
    val adHmToOneDStream: DStream[((String, String), Long)] = windowAdsLOgDStreeam.map(adsLog => {

      val hm: String = sdf.format(new Date(adsLog.timestamp))

      ((adsLog.adid, hm), 1L)
    })
    //3.统计总数 ((adid,hm),1L)=>((adid,hm),sum) reduceBykey(_+_)
    val adHmToCountDStream: DStream[((String, String), Long)] = adHmToOneDStream.reduceByKey(_ + _)
    //4.转换数据结构 ((adid,hm),sum)=>(adid,(hm,sum)) map()
    val adToHmCountDStream: DStream[(String, (String, Long))] = adHmToCountDStream.map {
      case ((adid, hm), count) => {
        (adid, (hm, count))
      }
    }
    //5.按照adid分组 (adid,(hm,sum))=>(adid,Iter[(hm,sum),...]) groupByKey
    adToHmCountDStream.groupByKey().mapValues(iter=>{
      iter.toList.sortWith(_._1<_._1)
    })



  }


}
