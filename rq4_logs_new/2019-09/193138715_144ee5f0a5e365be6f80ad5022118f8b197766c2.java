package com.sdk.java.dmm.api;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class ApiTestBase<T extends AbstractDmm> {

  /** プロパティ */
  private final static Properties PROPERTIES;
  /** プロパティ名 */
  private final static String PROPERTIES_FILE_NAME = "test.properties";

  static {
    try (InputStream is = ApiTestBase.class.getClassLoader()
        .getResourceAsStream(PROPERTIES_FILE_NAME);
        BufferedReader br = new BufferedReader(
            new InputStreamReader(is, StandardCharsets.UTF_8))) {
      PROPERTIES = new Properties();
      PROPERTIES.load(br);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * API_IDを取得する。
   *
   * @return API_ID
   */
  public static String getApiId() {
    return PROPERTIES.getProperty("API_ID");
  }

  /**
   * アフィリエイトIDを取得する。
   *
   * @return アフィリエイトID
   */
  public static String getAffiliateId() {
    return PROPERTIES.getProperty("AFFILIATE_ID");
  }

  /**
   * 検索オブジェクトを生成します。
   *
   * @param clazz 検索クラス
   * @return 検索オブジェクト
   */
  public T create(Class<T> clazz) {
    try {
      Constructor<T> constructor = clazz.getConstructor(String.class, String.class);
      return constructor.newInstance(getApiId(), getAffiliateId());
    } catch (Exception e) {
      throw new RuntimeException();
    }
  }

}