package com.interview.util;


import org.apache.commons.codec.digest.DigestUtils;

/**
 * 字符串加密工具类.
 * 用于加密一些隐秘的数据(单向加密).
 * 包括 md5 加密，重复 md5 加密，sha256 加密(64 位) 字符串,sha512 加密(128 位).
 * <p>
 * 依赖：
 * apache 家的 commons-codec 加密类库.
 *
 * @author rxliuli
 */
public class EncryptUtil {
  /**
   * 私有化构造器
   */
  private EncryptUtil() {
  }

  /**
   * md5 加密
   *
   * @param value 要加密的值
   * @return md5 加密后的值
   */
  public static String md5Hex(String value) {
    return DigestUtils.md5Hex(value);
  }

  /**
   * 多次 md5 操作
   *
   * @param value            要加密的值
   * @param md5EncryptNumber 要进行 md5 加密的次数
   * @return 多次 md5 加密后的值
   */
  public static String md5Hex(String value, int md5EncryptNumber) {
    for (int i = 0; i < md5EncryptNumber; i++) {
      value = DigestUtils.md5Hex(value);
    }
    return value;
  }

  /**
   * sha256 加密
   *
   * @param value 要加密的值
   * @return sha256 加密后的值
   */
  public static String sha256Hex(String value) {
    return DigestUtils.sha256Hex(value);
  }

  /**
   * sha512 加密
   *
   * @param value 要加密的值
   * @return sha512 加密后的值
   */
  public static String sha512Hex(String value) {
    return DigestUtils.sha512Hex(value);
  }
}