package com.tenghe.utils;

import com.tenghe.utils.model.DataCheckResult;
import com.tenghe.utils.model.DataCheckRule;

import org.apache.log4j.Logger;

import java.sql.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * author: teng.he
 * time: 20:29 2017/8/22
 * desc: 数据检测工具类
 *
 *
 * 标准中数据格式中使用的字符含义如下：
 * c=字符
 * n=数字
 * ..ul=长度不确定的文本
 * ..p,q(p,q均为自然数) =最长p个数字字符，小数点后q位
 * ..=从最小长度到最大长度，前面附加最小长度，后面附加最大长度
 * YYYYMMDDhhmmss=“YYYY”表示年份，“MM”表示月份，“DD”表示日期，“hh”表示小时，“mm”表示分钟，“ss”表示秒可以视实际情况组合使用。
 * 示例1：c5表示定长5个字符
 * 示例2：n..17，2   表示最长17个数字字符，小数点后两位。
 * 示例3：c3..8     表示最大长度为8，最小长度为3的不定长的字符。
 */
public final class DataCheckUtil {
  private static final Logger LOGGER = Logger.getLogger(DataCheckUtil.class);
  private static final String TYPE_ERROR_MSG = "数据类型与标准不符";
  private static final String LENGTH_ERROR_MSG = "数据长度与标准不符";
  private static final String CODE_ERROR_MSG = "数据代码与标准不符";
  private static final String INPUT_STRING_REGEX =
      "(?<type>c)((?<len>\\d+)|((?<min>\\d*)\\.\\.(?<max>\\d+)))";
  private static final String INPUT_NUMERIC_REGEX =
      "(?<type>n)((?<len>\\d+)|((?<min>\\d*)\\.\\.(?<max>\\d+)))(\\s*,\\s*(?<decimal>\\d+))?";
  private static final Pattern INPUT_STRING_PATTERN = Pattern.compile(INPUT_STRING_REGEX);
  private static final Pattern INPUT_NUMERIC_PATTERN = Pattern.compile(INPUT_NUMERIC_REGEX);

  private DataCheckUtil() {}

  private static DataCheckRule getStringRule(String expressionFormat) {
    DataCheckRule rule = new DataCheckRule();
    Matcher matcher = INPUT_STRING_PATTERN.matcher(expressionFormat);
    if (matcher.matches()) {
      rule.setType(matcher.group("type"));
      rule.setLen(matcher.group("len"));
      rule.setMin(matcher.group("min"));
      rule.setMax(matcher.group("max"));
    }
    return rule;
  }

  private static DataCheckRule getNumericRule(String expressionFormat) {
    DataCheckRule rule = new DataCheckRule();
    Matcher matcher = INPUT_NUMERIC_PATTERN.matcher(expressionFormat);
    if (matcher.matches()) {
      rule.setType(matcher.group("type"));
      rule.setLen(matcher.group("len"));
      rule.setMin(matcher.group("min"));
      rule.setMax(matcher.group("max"));
      rule.setDecimal(matcher.group("decimal"));
    }
    return rule;
  }

  public static boolean isNumber(String str){
    String reg = "^[0-9]+(.[0-9]+)?$";
    return str.matches(reg);
  }

  /**
   * 检测boolean类型的数据
   * @param columnValue
   * @return
   */
  private static DataCheckResult checkBoolean(Object columnValue) {
    DataCheckResult dataCheckResult = new DataCheckResult();
    if (columnValue instanceof Boolean) {
      dataCheckResult.setValid(true);
    } else {
      dataCheckResult.setValid(false);
      dataCheckResult.setErrMsg(TYPE_ERROR_MSG);
    }
    return dataCheckResult;
  }

  /**
   * 检查日期
   * @param columnValue
   * @return
   */
  private static DataCheckResult checkDate(Object columnValue) {
    DataCheckResult dataCheckResult = new DataCheckResult();
    if (!checkDataType(String.valueOf(columnValue))) {
      dataCheckResult.setValid(false);
      dataCheckResult.setErrMsg(TYPE_ERROR_MSG);
      return dataCheckResult;
    }

    dataCheckResult.setValid(true);
    return dataCheckResult;
  }

  private static boolean checkDataType(String value) {
    try {
      Date.valueOf(value);
    } catch (IllegalArgumentException e) {
      LOGGER.warn("DataCheckUtil checkDataType IllegalArgumentException value: " + value
          + "message:" + e.getMessage(), e);
      return false;
    }
    return true;
  }

  /**
   * 检测字符串
   * @param columnValue
   * @param expressionFormat
   * @return
   */
  private static DataCheckResult checkString(Object columnValue, String expressionFormat, List<String> codes) {
    DataCheckResult dataCheckResult = new DataCheckResult();
    String value = String.valueOf(columnValue);
    if (!containsCode(codes, value)) {
      dataCheckResult.setValid(false);
      dataCheckResult.setErrMsg(CODE_ERROR_MSG);
      return dataCheckResult;
    }

    if (!checkStringLength(value.length(), expressionFormat)) {
      dataCheckResult.setValid(false);
      dataCheckResult.setErrMsg(LENGTH_ERROR_MSG);
      return dataCheckResult;
    }

    dataCheckResult.setValid(true);
    return dataCheckResult;
  }

  private static boolean checkStringLength(int length, String expressionFormat) {
    DataCheckRule rule = getStringRule(expressionFormat);
    Integer len = rule.getLen();
    Integer min = rule.getMin();

    if (min == null) {
      min = 0;
    }

    //定长字符串 c3
    if (len != null) {
      return length == len;
    }

    //不定长字符串 c3..8 c..8
    Integer max = rule.getMax();
    return (length >= min) && (length <= max);
  }

  private static boolean containsCode(List<String> codes, String value) {
    return codes == null || codes.isEmpty() || codes.contains(value);
  }

  /**
   * 检测数字
   * @param columnValue
   * @param expressionFormat
   * @return
   */
  private static DataCheckResult checkNumeric(Object columnValue, String expressionFormat, List<String> codes) {
    DataCheckResult dataCheckResult = new DataCheckResult();
    String value = String.valueOf(columnValue);
    if (!containsCode(codes, value)) {
      dataCheckResult.setValid(false);
      dataCheckResult.setErrMsg(CODE_ERROR_MSG);
      return dataCheckResult;
    }

    if (!isNumber(value)) {
      dataCheckResult.setValid(false);
      dataCheckResult.setErrMsg(TYPE_ERROR_MSG);
      return dataCheckResult;
    }

    if (!checkNumericLength(value, expressionFormat)) {
      dataCheckResult.setValid(false);
      dataCheckResult.setErrMsg(LENGTH_ERROR_MSG);
      return dataCheckResult;
    }

    dataCheckResult.setValid(true);
    return dataCheckResult;
  }

  private static boolean checkNumericLength(String value, String expressionFormat) {

    DataCheckRule rule = getNumericRule(expressionFormat);
    Integer len = rule.getLen();
    Integer decimal = rule.getDecimal();

    int length = value.length();
    //定长数字
    if (len != null) {
      // 定长数字 n3
      if (decimal == null) {
        return length == len;
      }
      // 定长数字 n17,3
      return (length == len) && (getDecimal(value) == decimal);
    }

    Integer max = rule.getMax();
    Integer min = rule.getMin();
    if (min == null) {
      min = 0;
    }

    //不定长数字 n3..8 n..8
    if (decimal == null) {
      return (length >= min) && (length <= max);
    }
    //不定长数字 n3..8,2  n..17,3
    return (length >= min) && (length <= max) && (getDecimal(value) == decimal);
  }

  private static int getDecimal(String value) {
    return value.substring(value.indexOf('.') + 1).length();
  }
}