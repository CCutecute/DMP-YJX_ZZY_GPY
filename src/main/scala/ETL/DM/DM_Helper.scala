package ETL.DM
import constants.ComConstants
import scala.collection.mutable.ArrayBuffer

/**
 * @description 一些复用的列
 * @author 郭鹏野
 * @data 2019/12/06.
 * @version 1.0
 */
object DM_Helper {
  def select_dm_expr(): ArrayBuffer[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=(ComConstants.PROVINCE)
    columns.+=(ComConstants.CITY)
    columns.+=(ComConstants.RAW_REQUEST)
    columns.+=(ComConstants.VALID_REQUEST)
    columns.+=(ComConstants.AD_REQUEST)
    columns.+=(ComConstants.BIDDING_NUM)
    columns.+=(ComConstants.SUCESS_NUM)
    columns.+=(ComConstants.SUCCESS_RATE)
    columns.+=(ComConstants.SHOW_NUM)
    columns.+=(ComConstants.CLICK_NUM)
    columns.+=(ComConstants.CLICK_RATE)
    columns.+=(ComConstants.AD_COST)
    columns.+=(ComConstants.AD_SPEND)

    columns

  }
}
