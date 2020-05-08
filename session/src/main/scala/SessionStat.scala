import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils.{DateUtils, ParamUtils, StringUtils, ValidUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SessionStat {


  def main(args: Array[String]): Unit = {


    //获取筛选条件
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)

    //获取筛选条件对应的JsonObject
    val taskParam = JSONObject.fromObject(jsonStr)


    //创建全局唯一主键
    val taskUUID = UUID.randomUUID().toString

    val sparkConf: SparkConf = new SparkConf().setAppName("session").setMaster("local[*]")


    //创建sparkSession（包含SparkContext）
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //获取原始的动作表数据
    val actionRDD = getOriActionRDD(sparkSession, taskParam)

    //    actionRDD.foreach(println(_))

    //sessionId2ActionRDD:RDD[(sessionId,UserVisitAction)]
    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = actionRDD.map(item => (item.session_id, item))


    //session2GroupActionRDD:RDD[(sessionId,itreable_UserVisitAction)]
    val session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey()

    session2GroupActionRDD.cache()


    val sessionId2FullInfoRDD = getSessionFullInfo(sparkSession, session2GroupActionRDD)

    val sessionId2FilterRDD = getSessionFilteredRDD(taskParam, sessionId2FullInfoRDD)

    sessionId2FilterRDD.foreach(println(_))

  }

  def getSessionFilteredRDD(taskParam: JSONObject, sessionId2FullInfoRDD: RDD[(String, String)]) = {


    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professsional = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    var filterInfo = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professsional != null) Constants.PARAM_PROFESSIONALS + "=" + professsional + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds + "|" else "")


    if (filterInfo.endsWith("\\|")) {
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)  }


      sessionId2FullInfoRDD.filter {


        case (sessId, fullInfo) =>
          var sucess = true
          if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
            sucess = false


          } else if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)) {
            sucess = false
          } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)) {
            sucess = false
          } else if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)) {
            sucess = false
          } else if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.FIELD_SEARCH_KEYWORDS)) {
            sucess = false
          } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CLICK_CATEGORY_IDS, filterInfo, Constants.FIELD_CLICK_CATEGORY_IDS)) {
            sucess = false
          }

          if (sucess) {

            //acc.add()

          }

          sucess
      }





  }

  def getSessionFullInfo(sparkSession: SparkSession,
                         session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])]) = {
    //userId2AggrInfoRDD:RR[(userId,aggrInfo)]
    val userId2AggrInfoRDD = session2GroupActionRDD.map {

      case (sessionId, iterableAction) =>

        var userId = -1L


        var startTime: Date = null
        var endTime: Date = null


        var stepLength = 0

        val searchKeywords = new StringBuffer("")
        val clickCategories = new StringBuilder("")

        for (action <- iterableAction) {


          if (userId == -1L) {
            userId = action.user_id
          }


          val actionTime = DateUtils.parseTime(action.action_time)
          if (startTime == null || startTime.after(actionTime)) {

            startTime = actionTime

          }

          if (endTime == null || endTime.before(actionTime)) {

            endTime = actionTime
          }


          val searchKeyword = action.search_keyword
          if (StringUtils.isNotEmpty(searchKeyword) && !searchKeywords.toString.contains(searchKeyword)) {
            searchKeywords.append(searchKeyword + ",")
          }


          val clickCategoryId = action.click_category_id
          if (clickCategoryId != -1 && !clickCategories.toString().contains(clickCategoryId)) {

            clickCategories.append(clickCategoryId + ",")

          }

          stepLength += 1
        }

        val searchKw = StringUtils.trimComma(searchKeywords.toString)
        val clickCg = StringUtils.trimComma(clickCategories.toString)

        val vilistLenth = (endTime.getTime - startTime.getTime) / 1000


        val aggrInfo =
          Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
            Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
            Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
            Constants.FIELD_VISIT_LENGTH + "=" + vilistLenth + "|" +
            Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
            Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)

        (userId, aggrInfo)

    }

    val sql = "select * from user_info"


    import sparkSession.implicits._
    //userId2InfoRDD:RDD[(user_id,userInfo)]
    val userId2InfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item))


    val sessionId2FullInfoRDD: RDD[(String, String)] = userId2AggrInfoRDD.join(userId2InfoRDD).map {

      case (userId, (aggrInfo, userInfo)) =>

        val age = userInfo.age
        val professional = userInfo.professional
        val sex = userInfo.sex
        val city = userInfo.city
        val fullInfo =
          aggrInfo + "|" +
            Constants.FIELD_AGE + "=" + age + "|" +
            Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
            Constants.FIELD_SEX + "=" + professional + "|" +
            Constants.FIELD_CITY + "=" + city


        val sessionId = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)
        (sessionId, fullInfo)
    }

    sessionId2FullInfoRDD


  }
  def getOriActionRDD(sparkSession: SparkSession, taskParam: JSONObject) = {

    val startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)

    val sql ="select * from user_visit_action where date>='" + startDate+"' and date<='" + endDate+ "'"
//    val sql ="select * from user_visit_action"


    import sparkSession.implicits._

     sparkSession.sql(sql).as[UserVisitAction].rdd


  }


}
