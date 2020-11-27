package com.nswdwy.handler

import java.sql.{Connection, Date}
import java.text.SimpleDateFormat

import com.nswdwy.app.Ads_log
import com.nswdwy.util.JDBCUtil
import org.apache.spark.streaming.dstream.DStream

/**
 * @author yycstart
 * @create 2020-11-25 21:08
 */
object DateAreaCityAdCountHandler {

  // 时间格式化对象
  private val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  // 根据黑名单过滤后的数据集，统计每天各大区各个城市广告点击总数并保存至MySQL中
  def saveDateAreaCityAdCountToMysql(filterAdsLogDStream: DStream[Ads_log]): Unit = {

    //1.统计每天各大区各个城市广告点击总数
    val dateAreaCityAdToCount: DStream[((String, String, String, String), Long)] = filterAdsLogDStream.map(ads_log => {

      //a.格式化为日期字符串
      val dt: String = sdf.format(new Date(ads_log.timestamp))

      //b.组合,返回
      ((dt, ads_log.area, ads_log.city, ads_log.adid), 1L)
    }).reduceByKey(_ + _)

    //2.将单个批次统计之后的数据集合MySQL数据对原有的数据更新
    dateAreaCityAdToCount.foreachRDD(rdd => {

      //对每个分区单独处理
      rdd.foreachPartition(iter => {
        //a.获取连接
        val connection: Connection = JDBCUtil.getConnection

        //b.写库
        iter.foreach { case ((dt, area, city, adid), ct) =>
          JDBCUtil.executeUpdate(
            connection,
            """
              |INSERT INTO area_city_ad_count (dt,area,city,adid,count)
              |VALUES(?,?,?,?,?)
              |ON DUPLICATE KEY
              |UPDATE count=count+?;
                        """.stripMargin,
            Array(dt, area, city, adid, ct, ct)
          )
        }

        //c.释放连接
        connection.close()
      })
    })
  }
}
