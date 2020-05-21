import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}


object AreaTop3Stat {



  def main(args: Array[String]): Unit = {

    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)


    val taskParam = JSONObject.fromObject(jsonStr)

    val taskUUID = UUID.randomUUID().toString


    val sparkConf = new SparkConf().setAppName("area").setMaster("local[*]")

    val sprakSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //RDD[(cityId,pid)]
    val cityId2PidRDD = getCityAndProductInfo(sprakSession, taskParam)


    //RDD[(cityId,CityAreaInfo)]
    val cityId2AreaInfoRDD = getCityAreaInfo(sprakSession)


    //tmp_area_basic_info 表中的一条数据就是一个点击行为
    getAreaPidBasicTable(sprakSession, cityId2PidRDD, cityId2AreaInfoRDD)




    sprakSession.udf.register("concat_long_string",(v1:Long,v2:String,split:String) =>
    {
     v1 + split + v2
    })

    sprakSession.udf.register("group_concat_distinct",new GroupConcatDistinct)


    getAreaProductClickCountTable(sprakSession)

    sprakSession.udf.register("get_json_field",(json:String,field:String) => {


      val jsonObject = JSONObject.fromObject(json)
      jsonObject.getString(field)

    })

    getAreaProductClickCountInfo(sprakSession)


    getTop3Product(sprakSession,taskUUID)



  }


  def getTop3Product(sprakSession: SparkSession, taskUUID: String) = {

//
//    val sql = "select area,city_infos,pid,product_name,product_status,click_count,"+
//    "row_number() over(PARTITION BY area ORDER BY click_count DESC) rank from tmp_area_count_product_info"
//
//
//    sprakSession.sql(sql).createOrReplaceTempView("temp_test")


    val sql = "select area, " +
      "CASE " +
      "WHEN area='华北' OR area='华东' THEN 'A_Level' " +
      "WHEN area='华中' OR area='华南' THEN 'B_Level' " +
      "WHEN area='西南' OR area='西北' THEN 'C_Level' " +
      "ELSE 'D_Level' " +
      "END area_level, " +
      "city_infos, pid, product_name, product_status, click_count from (" +
      "select area, city_infos, pid, product_name, product_status, click_count, " +
      "row_number() over(PARTITION BY area ORDER BY click_count DESC) rank from " +
      "tmp_area_count_product_info) t where rank<=3"


       val top3ProductRDD = sprakSession.sql(sql).rdd.map {

          case row =>
            AreaTop3Product(taskUUID,row.getAs[String]("area"),row.getAs[String]("area_level"),
              row.getAs[Long]("pid"),row.getAs[String]("city_infos"),
              row.getAs[Long]("click_count"),row.getAs[String]("product_name"),
              row.getAs[String]("product_status")
            )}


    import sprakSession.implicits._
    top3ProductRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "area_top_product")
      .mode(SaveMode.Append)
      .save()
  }


  def getAreaProductClickCountInfo(sprakSession: SparkSession) = {

    //tmp_area_click_count: area,city_infos,pid,click_count tacc
    //product_info:product_id,product_name,extend_info pi

    val sql = "select tacc.area, tacc.city_infos, tacc.pid, pi.product_name, " +
      "if(get_json_field(pi.extend_info, 'product_status')='0','Self','Third Party') product_status," +
      "tacc.click_count " +
      " from tmp_area_click_count tacc join product_info pi on tacc.pid = pi.product_id"



    sprakSession.sql(sql).createOrReplaceTempView("tmp_area_count_product_info")



  }




  def getAreaProductClickCountTable(sparkSession: SparkSession) = {

    val sql = "select area,pid,count(*) click_count,group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos from tmp_area_basic_info group by area,pid"

    sparkSession.sql(sql).createOrReplaceTempView("tmp_area_click_count")
  }


  def getCityAreaInfo(sprakSession: SparkSession) = {

    val cityAreaInfoArray = Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"),
      (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"),
      (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"),
      (9L, "哈尔滨", "东北"))


    //RDD[(cityId,CityAreaInfo)]
    sprakSession.sparkContext.makeRDD(cityAreaInfoArray).map{

      case (cityId,cityName,area) =>


        (cityId, CityAreaInfo(cityId, cityName, area))
    }


  }

  def getAreaPidBasicTable(sprakSession: SparkSession,
                           cityId2PidRDD: RDD[(Long, Long)],
                           cityId2AreaInfoRDD: RDD[(Long, CityAreaInfo)]) =
    {

      val areaPidInfoRDD = cityId2PidRDD.join(cityId2AreaInfoRDD).map{

        case (cityId,(pid,areaInfo)) =>
          (cityId,areaInfo.city_name,areaInfo.area,pid)

      }

      import sprakSession.implicits._
      areaPidInfoRDD.toDF("city_id","city_name","area","pid").createOrReplaceTempView("tmp_area_basic_info")



    }


  def getCityAndProductInfo(sprakSession: SparkSession, taskParam: JSONObject) = {

    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)


    //获取发生过点击的action数据
    //获取到的一条action就代表一个点击行为
    val sql = "select city_id,click_product_id from user_visit_action where date >= '" + startDate + "' and date <= '" + endDate + "' and click_product_id != -1"

    import sprakSession.implicits._
    sprakSession.sql(sql).as[CityClickProduct].rdd.map {

      case cityPid => (cityPid.city_id, cityPid.click_product_id)

    }


  }
}
