import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object PageConvertStat {



  def main(args: Array[String]): Unit = {


    //获取任务限制条件
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)

    val taskParam = JSONObject.fromObject(jsonStr)


    //获取唯一主键
    val taskUUID = UUID.randomUUID().toString

    //创建sparkconf
    val sparkConf = new SparkConf().setAppName("pageConvert").setMaster("local[*]")

    //创建sprakSession
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //获取用户行为数据
    val sessionId2ActionRDD = getUserVisitAction(sparkSession, taskParam)

    //pageFlowStr:"1,2,3,4,5,6,7"
    val pageFlowStr = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)

    //pageFlowArray:Array[Long] [1,2,3,4,5,6,7]
    val pageFlowArray = pageFlowStr.split(",")

    //pageFlowArray.slice(0, pageFlowArray.length - 1):[1,2,3,4,5,6]
    //pageFlowArray.tail: [2,3,4,5,6,7]
    //pageFlowArray.slice(0, pageFlowArray.length - 1).zip(pageFlowArray.tail):[(1,2),(2,3)....]
    //targetPageSplit:[1_2,2_3,3_4,......]
    val targetPageSplit = pageFlowArray.slice(0, pageFlowArray.length - 1).zip(pageFlowArray.tail).map{
      case (page1,page2) => page1 + "_" + page2
    }


    //sessionId2ActionRDD:RDD[(session,action)]
    val sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey()


    //pageSplitNumRDD:RDD[(string,1L)]
    val pageSplitNumRDD: RDD[(String, Long)] = sessionId2GroupRDD.flatMap {

      case (sessionId, iterableAction) =>
        val sortList = iterableAction.toList.sortWith((item1, item2) => {

          //item1:action
          //item2:action
          //sortList:List[UserVisitAction]
          DateUtils.parseTime(item1.action_time).getTime < DateUtils.parseTime(item2.action_time).getTime
        })


        val pageList = sortList.map {

          case action => action.page_id

        }

        //pageList.slice(0, pageList.length - 1):[2,3,4,....N]
        //pageList.tail:[2,3,4,....N]
        //pageList.slice(0, pageList.length - 1).zip(pageList.tail):[(1,2),(2,3),.....]
        //pageSplit:[1_2,2_3,....]
        val pageSplit = pageList.slice(0, pageList.length - 1).zip(pageList.tail).map {
          case (page1, page2) => page1 + "_" + page2

        }

        val pageSplitFilter = pageSplit.filter {

          case pageSplit => targetPageSplit.contains(pageSplit)

        }

        pageSplitFilter.map {

          case pageSplit => (pageSplit, 1L)
        }


    }


    //pageSplitCountMap:Map[(pageSplit,count)]
    val pageSplitCountMap: collection.Map[String, Long] = pageSplitNumRDD.countByKey()

    val startPage = pageFlowArray(0).toLong

   val startPageCount =  sessionId2ActionRDD.filter{
      case (sessionId,action) => action.page_id ==  startPage
    }.count()



  }

  def getUserVisitAction(sparkSession: SparkSession, taskParam: JSONObject)= {

    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)


    val sql = "select * from user_visit_action where date >='" + startDate + "' and date <= '" + endDate + "'"

    import sparkSession.implicits._

    sparkSession.sql(sql).as[UserVisitAction].rdd.map(item => (item.session_id, item))


  }
}
