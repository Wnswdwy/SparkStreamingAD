package com.nswdwy.handler

import java.sql.{Connection, Date}
import java.text.SimpleDateFormat

import com.nswdwy.app.Ads_log
import com.nswdwy.util.JDBCUtil
import org.apache.spark.streaming.dstream.DStream

/**
 * @author yycstart
 * @create 2020-11-25 19:57
 */
object BlackListHandler {
  //时间格式化对象
  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  def addBlackList(filterAdsLogDSteam: DStream[Ads_log]): Unit = {

    //统计当前批次中单日每个用户点击每个广告的总次数
    //1.转换和累加：ads_log=>((date,user,adid),1) =>((date,user,adid),count)
    val dateUserAdToCount: DStream[((String, String, String), Long)] = filterAdsLogDSteam.map(
      adsLog => {

        //a.将时间戳转换为日期字符串
        val date: String = sdf.format(new Date(adsLog.timestamp))

        //b.返回值
        ((date, adsLog.userid, adsLog.adid), 1L)
      }
    ).reduceByKey(_ + _)

    //2 写出
    dateUserAdToCount.foreachRDD(
      rdd => {
        // 每个分区数据写出一次
        rdd.foreachPartition(
          iter => {
            // 获取连接
            val connection: Connection = JDBCUtil.getConnection

            iter.foreach { case ((dt, user, ad), count) =>
              // 向MySQL中user_ad_count表，更新累加点击次数
              JDBCUtil.executeUpdate(
                connection,
                """
                  |INSERT INTO user_ad_count (dt,userid,adid,count)
                  |VALUES (?,?,?,?)
                  |ON DUPLICATE KEY
                  |UPDATE count=count+?
                                """.stripMargin, Array(dt, user, ad, count, count)
              )

              // 查询user_ad_count表，读取MySQL中点击次数
              val ct: Long = JDBCUtil.getDataFromMysql(
                connection,
                """
                  |select count from user_ad_count where dt=? and userid=? and adid =?
                  |""".stripMargin,
                Array(dt, user, ad)
              )

              // 点击次数>30次，加入黑名单
              if (ct >= 30000) {
                JDBCUtil.executeUpdate(
                  connection,
                  """
                    |INSERT INTO black_list (userid) VALUES (?) ON DUPLICATE KEY update userid=?
                    |""".stripMargin,
                  Array(user, user)
                )
              }
            }

            connection.close()
          }
        )
      }
    )
  }

  // 判断用户是否在黑名单中
  def filterByBlackList(adsLogDStream: DStream[Ads_log]): DStream[Ads_log] = {

    adsLogDStream.filter(
      adsLog => {
        // 获取连接
        val connection: Connection = JDBCUtil.getConnection

        // 判断黑名单中是否存在该用户
        val bool: Boolean = JDBCUtil.isExist(
          connection,
          """
            |select * from black_list where userid=?
            |""".stripMargin,
          Array(adsLog.userid)
        )

        // 关闭连接
        connection.close()

        // 返回是否存在标记
        !bool
      }
    )
  }
}
