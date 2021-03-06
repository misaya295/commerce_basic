import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils.{DateUtils, NumberUtils, ParamUtils, StringUtils, ValidUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

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

    val sessionAccumulator = new SessionAccumulator
    sparkSession.sparkContext.register(sessionAccumulator)

    //获取原始的动作表数据
    val actionRDD = getOriActionRDD(sparkSession, taskParam)

    //    actionRDD.foreach(println(_))

    //sessionId2ActionRDD:RDD[(sessionId,UserVisitAction)]
    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = actionRDD.map(item => (item.session_id, item))


    //session2GroupActionRDD:RDD[(sessionId,itreable_UserVisitAction)]
    val session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey()


    session2GroupActionRDD.cache()


    val sessionId2FullInfoRDD = getSessionFullInfo(sparkSession, session2GroupActionRDD)

    //sessionId2FilterRDD[(sessionId,fullInfo)]是所有符合过滤条件的数据组成的RDD
    //getSessionFilteredRDD：实现根据限制条件对session数据进行guol，并完成累加器更新
    //过滤用户信息
    val sessionId2FilterRDD = getSessionFilteredRDD(taskParam, sessionId2FullInfoRDD, sessionAccumulator)


    //    sessionId2FilterRDD.foreach(println(_))


    //获取最后再难过统计结果
    getSessionRatio(sparkSession, taskUUID, sessionAccumulator.value)


    /**
     * 需求二:session随机抽取
     * //sessionId2FilterRDD[(sessionId,fullInfo)],一个session对应一条数据
     */
    sessionRandomExtarct(sparkSession, taskUUID, sessionId2FilterRDD)


    //sessionId2ActionRDD:RDD[(sessionId,action)]
    //sessionIdFilterRDD:RDD[(sessionId,FullInfo)]
    //sessionId2FilterActionRDD.join
    val sessionIdFilterActionRDD = sessionId2ActionRDD.join(sessionId2FilterRDD).map {


      case (sessionId, (action, fullInfo)) =>
        (sessionId, action)
    }


  val top10CategoryArray =  top10PopularCategories(sparkSession, taskUUID, sessionId2ActionRDD)

    top10ActiveSession(sparkSession,taskUUID,sessionIdFilterActionRDD,top10CategoryArray)



  }

  def top10ActiveSession(sparkSession: SparkSession,
                         taskUUID: String,
                         sessionId2FilterActionRDD: RDD[(String, UserVisitAction)],
                         top10CategoryArray: Array[(SortKey, String)]): Unit = {
    // 第一步：过滤出所有点击过Top10品类的action
    // 1： join
    //    val cid2CountInfoRDD = sparkSession.sparkContext.makeRDD(top10CategoryArray).map{
    //      case (sortKey, countInfo) =>
    //        val cid = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
    //        (cid, countInfo)
    //    }
    //
    //      val cid2ActionRDD = sessionId2FilterActionRDD.map{
    //        case (sessionId, action) =>
    //          val cid = action.click_category_id
    //          (cid, action)
    //      }
    //
    //    val sessionId2ActionRDD = cid2CountInfoRDD.join(cid2ActionRDD).map{
    //      case (cid, (countInfo, action)) =>
    //        val sid = action.session_id
    //        (sid, action)
    //    }

    // 2：使用filter
    // cidArray: Array[Long] 包含了Top10热门品类ID
    val cidArray = top10CategoryArray.map{
      case (sortKey, countInfo) =>
        val cid = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        cid
    }

    // 所有符合过滤条件的，并且点击过Top10热门品类的action
    val sessionId2ActionRDD = sessionId2FilterActionRDD.filter{
      case (sessionId, action) =>
        cidArray.contains(action.click_category_id)
    }

    // 按照sessionId进行聚合操作
    val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()

    // cid2SessionCountRDD: RDD[(cid, sessionCount)]
    val cid2SessionCountRDD = sessionId2GroupRDD.flatMap{
      case (sessionId, iterableAction) =>

        val categoryCountMap = new mutable.HashMap[Long, Long]()

        for(action <- iterableAction){
          val cid = action.click_category_id
          if(!categoryCountMap.contains(cid))
            categoryCountMap += (cid->0)
          categoryCountMap.update(cid, categoryCountMap(cid) + 1)
        }

        // 记录了一个session对于它所有点击过的品类的点击次数
        for((cid, count) <- categoryCountMap)
          yield (cid, sessionId + "=" + count)
    }

    // cid2GroupRDD: RDD[(cid, iterableSessionCount)]
    // cid2GroupRDD每一条数据都是一个categoryid和它对应的所有点击过它的session对它的点击次数
    val cid2GroupRDD = cid2SessionCountRDD.groupByKey()

    // top10SessionRDD: RDD[Top10Session]
    val top10SessionRDD = cid2GroupRDD.flatMap{
      case (cid, iterableSessionCount) =>
        // true: item1放在前面
        // flase: item2放在前面
        // item: sessionCount   String   "sessionId=count"
        val sortList = iterableSessionCount.toList.sortWith((item1, item2) => {
          item1.split("=")(1).toLong > item2.split("=")(1).toLong
        }).take(10)

        val top10Session = sortList.map{
          // item : sessionCount   String   "sessionId=count"
          case item =>
            val sessionId = item.split("=")(0)
            val count = item.split("=")(1).toLong
            Top10Session(taskUUID, cid, sessionId, count)
        }

        top10Session
    }

    import sparkSession.implicits._
    top10SessionRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_session_0308")
      .mode(SaveMode.Append)
      .save()

  }

  def getClickCount(sessionIdFilterActionRDD: RDD[(String, UserVisitAction)]) = {

    //    val clickFilterRDD = sessionIdFilterActionRDD.filter{
    //
    //      case (sessionId,action) => action.click_category_id != -1L
    //
    //
    //
    //    }

    //先进行过滤，把点击行为对应的action保留下来
    val clickFilterRDD =
      sessionIdFilterActionRDD.filter(item => item._2.click_category_id != -1L)

    //进行格式转换，为reduceByKey做准备
    val clickNumRDD = clickFilterRDD.map {


      case (sessionId, action) => (action.click_category_id, 1L)


    }

    clickNumRDD.reduceByKey(_ + _)

  }

  def getOrderCount(sessionIdFilterActionRDD: RDD[(String, UserVisitAction)]) = {
    val orderFilterRDD = sessionIdFilterActionRDD.filter(item => item._2.order_category_ids != null)

    val orderNumRDD = orderFilterRDD.flatMap {

      case (sessionId, action) => action.order_category_ids.split(",").map(item => (item.toLong, 1L))


    }

    orderNumRDD.reduceByKey(_ + _)


  }

  def getPayCount(sessionIdFilterActionRDD: RDD[(String, UserVisitAction)]) = {
    val payFilterRDD = sessionIdFilterActionRDD.filter(item => item._2.pay_category_ids != null)


    val payNumRDD = payFilterRDD.flatMap {

      case (sid, acito) =>
        acito.pay_category_ids.split(",").map(item => (item.toLong, 1L))


    }

    payNumRDD.reduceByKey(_ + _)

  }


  def top10PopularCategories(sparkSession: SparkSession, taskUUID: String,
                             sessionIdFilterActionRDD: RDD[(String, UserVisitAction)]) = {

    //第一步：获取所有发生过点击，下单，付款的品类


    var cid2CidRDD = sessionIdFilterActionRDD.flatMap {


      case (sid, action) =>
        val categoryBuffer = new ArrayBuffer[(Long, Long)]()

        //点击行为
        if (action.click_category_id != -1) {
          categoryBuffer += ((action.click_category_id, action.click_category_id))

        } else if (action.order_category_ids != null) {

          for (orderCid <- action.order_category_ids) {

            for (orderCid <- action.order_category_ids.split(",")) {
              categoryBuffer += ((orderCid.toLong, orderCid.toLong))
            }

          }

        } else if (action.order_category_ids != null) {
          for (paycid <- action.pay_category_ids.split(",")) {
            categoryBuffer += ((paycid.toLong, paycid.toLong))
          }
        }

        categoryBuffer


    }
    cid2CidRDD = cid2CidRDD.distinct()

    //第二步：统计品类点击次数,下单次数,付款次数
    val cid2ClickCountRDD = getClickCount(sessionIdFilterActionRDD)


    val cid2OrderCountRDD = getOrderCount(sessionIdFilterActionRDD)


    val cid2PayCountRDD = getPayCount(sessionIdFilterActionRDD)
    //cid2FullCountRDD[(cid,countInfo)]
    //(16,categoryid=16|clickCount=149|orderCount=136|payCount=139)
    val cid2FullCountRDD = getFullCount(cid2CidRDD, cid2ClickCountRDD, cid2OrderCountRDD, cid2PayCountRDD)


    //实现自定义二次排序key
    val sortKey2FullCountRDD: RDD[(SortKey, String)] = cid2FullCountRDD.map {

      case (cid, countInfo) =>
        val clickCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong


        val sortKey = SortKey(clickCount, orderCount, payCount)

        (sortKey, countInfo)


    }


    val top10CategoryArray: Array[(SortKey, String)] = sortKey2FullCountRDD.sortByKey(false).take(10)


    val top10CategoryRDD = sparkSession.sparkContext.makeRDD(top10CategoryArray).map {

      case (sortKey, countInfo) =>

        val cid = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        val clickCount = sortKey.clickCout
        val orderCount = sortKey.orderCount
        val payCount = sortKey.payCount


        Top10Category(taskUUID, cid, clickCount, orderCount, payCount)
    }
    import sparkSession.implicits._
    top10CategoryRDD.toDF.write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_category")
      .mode(SaveMode.Append)
      .save()

    top10CategoryArray
  }

  def getFullCount(cid2CidRDD: RDD[(Long, Long)], cid2ClickCountRDD: RDD[(Long, Long)],
                   cid2OrderCountRDD: RDD[(Long, Long)], cid2PayCountRDD: RDD[(Long, Long)]) = {


    val cid2ClickInfoRDD = cid2CidRDD.leftOuterJoin(cid2ClickCountRDD).map {


      case (cid, (categoryId, option)) =>

        val cilckCount = if (option.isDefined) option.get else ()
        val aggrCount = Constants.FIELD_CATEGORY_ID + "=" + cid + "|" +
          Constants.FIELD_CLICK_COUNT + "=" + cilckCount

        (cid, aggrCount)
    }

    //    cid2ClickInfoRDD.foreach(println(_))

    val cid2OrderInfoRDD = cid2ClickInfoRDD.leftOuterJoin(cid2OrderCountRDD).map {


      case (cid, (clickInfo, option)) =>
        val orderCount = if (option.isDefined) option get else ()

        val aggrInfo = clickInfo + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount


        (cid, aggrInfo)

    }


    val cid2PayInfoCountRDD = cid2OrderInfoRDD.leftOuterJoin(cid2PayCountRDD).map {

      case (cid, (orderinfo, option)) =>

        val payCount = if (option.isDefined) option.get else ()
        val aggrInfo = orderinfo + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount

        (cid, aggrInfo)

    }

    cid2PayInfoCountRDD
  }


  def generateRandomIndexList(extarctPerDay: Long, daySessionCount: Long, hourCountMap: mutable.HashMap[String, Long],
                              hourListMap: mutable.HashMap[String, ListBuffer[Int]]): Unit = {

    for ((hour, count) <- hourCountMap) {


      var hourExrCount = ((count / daySessionCount.toDouble) * extarctPerDay).toInt

      //避免一个小时要抽取的数量超过这个小时的总数
      if (hourExrCount > count) {

        hourExrCount = count.toInt

      }

      val random = new Random()

      hourListMap.get(hour) match {
        case None => hourListMap(hour) = new ListBuffer[Int]
          for (i <- 0 until hourExrCount) {


            var index = random.nextInt(count.toInt)

            while (hourListMap(hour).contains(index)) {
              index = random.nextInt(count.toInt)

            }

            hourListMap(hour).append(index)
          }

        case Some(list) =>
          for (i <- 0 until hourExrCount) {


            var index = random.nextInt(count.toInt)

            while (hourListMap(hour).contains(index)) {
              index = random.nextInt(count.toInt)

            }

            hourListMap(hour).append(index)
          }
      }
    }


  }


  def sessionRandomExtarct(sparkSession: SparkSession, taskUUID: String, sessionId2FilterRDD: RDD[(String, String)]): Unit = {

    //dateHour2FullInfoRDD[(dateHour,fullInfo)]
    val dateHour2FullInfoRDD: RDD[(String, String)] = sessionId2FilterRDD.map {

      case (sessionId, fullInfo) =>

        val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)

        //dateHour:yyyy-MM-dd_HH
        val dateHour = DateUtils.getDateHour(startTime)

        (dateHour, fullInfo)


    }

    //hourCountMap:Map[(dateHour,count)]
    val hourCountMap = dateHour2FullInfoRDD.countByKey()

    //fateHourCountMap:Map[(date,Map[(hour,count)])]
    val dateHourCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()


    for ((dateHour, count) <- hourCountMap) {


      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)


      dateHourCountMap.get(date) match {


        case None => dateHourCountMap(date) = new mutable.HashMap[String, Long]()
          dateHourCountMap(date) += (hour -> count)

        case Some(map) => dateHourCountMap(date) += (hour -> count)
      }


    }

    //解决问题一：一共有多少天：dataHourCountMap.size
    //          一天抽取多少条：100
    val extractPerDay = 100 / dateHourCountMap.size

    //解决问题二：一天有多少session:dataHourCountMap(date).value.sum
    //解决问题三：一小时有多少session：dateHourCountMap(date)(hour)

    val dataHourExtractIndexListMap = new mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]()

    //dateHourCountMap:Map([date,Map[(hour,count)])
    for ((date, hourCountMap) <- dateHourCountMap) {


      val dateSessionCount = hourCountMap.values.sum

      dataHourExtractIndexListMap.get(date) match {
        case None => dataHourExtractIndexListMap(date) = new mutable.HashMap[String, ListBuffer[Int]]()

          generateRandomIndexList(extractPerDay, dateSessionCount, hourCountMap, dataHourExtractIndexListMap(date))

        case Some(map) =>
          generateRandomIndexList(extractPerDay, dateSessionCount, hourCountMap, dataHourExtractIndexListMap(date))

      }

      //到目前为止，我们获得了每个小时要抽取的session的index
      //广播大变量，提升任务性能
      val dataHourExtractIndexListMapBd = sparkSession.sparkContext.broadcast(dataHourExtractIndexListMap)

      //dateHour2FullInfoRDD:RDD[(dateHour,fullInfo)]
      //dateHour2GroupRDD:RDD[(dateHour,iterableFullInfo)]
      val dateHour2GroupRDD: RDD[(String, Iterable[String])] = dateHour2FullInfoRDD.groupByKey()
      //extractSessionRDD
      val extractSessionRDD = dateHour2GroupRDD.flatMap {

        case (dateHour, iterableFullInfo) =>
          val date = dateHour.split("_")(0)
          val hour = dateHour.split("_")(1)


          val extractList = dataHourExtractIndexListMapBd.value.get(date).get(hour)

          val extractSessionArrayBuffer = new ArrayBuffer[SessionRandomExtract]()

          var index = 0

          for (fullInfo <- iterableFullInfo) {


            if (extractList.contains(index)) {

              val sessionId = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SESSION_ID)

              val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)


              val searchKeywords = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)

              val clickCategories = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)


              val extractSession = SessionRandomExtract(taskUUID, sessionId, startTime, searchKeywords, clickCategories)


              extractSessionArrayBuffer += extractSession
            }

            index += 1

          }
          extractSessionArrayBuffer


      }
      import sparkSession.implicits._
      extractSessionRDD.toDF().write
        .format("jdbc")
        .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
        .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
        .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
        .option("dbtable", "session_extract")
        .mode(SaveMode.Append)
        .save()
    }


  }


  def getSessionRatio(sparkSession: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]): Unit = {


    val session_count = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble
    val visitLength_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visitLength_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visitLength_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visitLength_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visitLength_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visitLength_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visitLength_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visitLength_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visitLength_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)


    val stepLength_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val stepLength_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val stepLength_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val stepLength_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val stepLength_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val stepLength_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)


    val visitLength_1s_3s_ratio = NumberUtils.formatDouble(visitLength_1s_3s / session_count, 2)
    val visitLength_4s_6s_ratio = NumberUtils.formatDouble(visitLength_4s_6s / session_count, 2)
    val visitLength_7s_9s_ratio = NumberUtils.formatDouble(visitLength_7s_9s / session_count, 2)
    val visitLength_10s_30s_ratio = NumberUtils.formatDouble(visitLength_10s_30s / session_count, 2)
    val visitLength_30s_60s_ratio = NumberUtils.formatDouble(visitLength_30s_60s / session_count, 2)
    val visitLength_1m_3m_ratio = NumberUtils.formatDouble(visitLength_1m_3m / session_count, 2)
    val visitLength_3m_10m_ratio = NumberUtils.formatDouble(visitLength_3m_10m / session_count, 2)
    val visitLength_10m_30m_ratio = NumberUtils.formatDouble(visitLength_10m_30m / session_count, 2)
    val visitLength_30m_ratio = NumberUtils.formatDouble(visitLength_30m / session_count, 2)


    val stepLength_1_3_ratio = NumberUtils.formatDouble(stepLength_1_3 / session_count, 2)
    val stepLength_4_6_ratio = NumberUtils.formatDouble(stepLength_4_6 / session_count, 2)
    val stepLength_7_9_ratio = NumberUtils.formatDouble(stepLength_7_9 / session_count, 2)
    val stepLength_10_30_ratio = NumberUtils.formatDouble(stepLength_10_30 / session_count, 2)
    val stepLength_30_60_ratio = NumberUtils.formatDouble(stepLength_30_60 / session_count, 2)
    val stepLength_60_ratio = NumberUtils.formatDouble(stepLength_60 / session_count, 2)


    val stat = SessionAggrStat(taskUUID, session_count.toInt, visitLength_1s_3s_ratio,
      visitLength_4s_6s_ratio, visitLength_7s_9s_ratio, visitLength_10s_30s_ratio, visitLength_30s_60s_ratio,
      visitLength_1m_3m_ratio, visitLength_3m_10m_ratio, visitLength_10m_30m_ratio, visitLength_30m_ratio, stepLength_1_3_ratio, stepLength_4_6_ratio
      , stepLength_7_9_ratio, stepLength_10_30_ratio, stepLength_30_60_ratio, stepLength_60_ratio)


    val sessionRatioRDD = sparkSession.sparkContext.makeRDD(Array(stat))

    import sparkSession.implicits._
    //    sessionRatioRDD.toDF().write
    //      .format("jdbc")
    //      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
    //      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
    //      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
    //      .option("dbtable","session_stat_ratio")
    //      .mode(SaveMode.ErrorIfExists)
    //      .save()


  }

  def calculateVisitLength(vistrLength: Long, sessionAccumulator: SessionAccumulator) = {


    if (vistrLength >= 1 && vistrLength <= 3) {
      sessionAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    }
    else if (vistrLength >= 7 && vistrLength <= 9) {
      sessionAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    }
    else if (vistrLength >= 10 && vistrLength <= 30) {
      sessionAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    }
    else if (vistrLength >= 30 && vistrLength <= 60) {
      sessionAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    }
    else if (vistrLength >= 60 && vistrLength <= 180) {
      sessionAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    }
    else if (vistrLength >= 180 && vistrLength <= 600) {
      sessionAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    }
    else if (vistrLength >= 600 && vistrLength <= 1800) {
      sessionAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    }
    else if (vistrLength > 1800) {
      sessionAccumulator.add(Constants.TIME_PERIOD_30m)
    }
  }

  def calculateStepLength(stepLength: Long, sessionAccumulator: SessionAccumulator) = {
    if (stepLength >= 1 && stepLength <= 3) {
      sessionAccumulator.add(Constants.STEP_PERIOD_1_3)
    }
    else if (stepLength >= 4 && stepLength <= 6) {
      sessionAccumulator.add(Constants.STEP_PERIOD_4_6)
    }
    else if (stepLength >= 7 && stepLength <= 9) {
      sessionAccumulator.add(Constants.STEP_PERIOD_7_9)
    }
    else if (stepLength >= 10 && stepLength <= 30) {
      sessionAccumulator.add(Constants.STEP_PERIOD_10_30)
    }
    else if (stepLength >= 30 && stepLength <= 60) {
      sessionAccumulator.add(Constants.STEP_PERIOD_30_60)
    }
    else if (stepLength > 60) {
      sessionAccumulator.add(Constants.STEP_PERIOD_60)
    }
  }

  def getSessionFilteredRDD(taskParam: JSONObject, sessionId2FullInfoRDD: RDD[(String, String)], sessionAccumulator: SessionAccumulator) = {

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
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)
    }


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
          sessionAccumulator.add(Constants.SESSION_COUNT)


          val vistrLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong


          calculateVisitLength(vistrLength, sessionAccumulator)
          calculateStepLength(stepLength, sessionAccumulator)


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

    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql = "select * from user_visit_action where date>='" + startDate + "' and date<='" + endDate + "'"
    //    val sql ="select * from user_visit_action"


    import sparkSession.implicits._

    sparkSession.sql(sql).as[UserVisitAction].rdd


  }
}