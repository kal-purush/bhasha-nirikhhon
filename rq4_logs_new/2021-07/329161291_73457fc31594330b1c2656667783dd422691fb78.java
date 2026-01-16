package com.bizseer.bigdata.common;

import com.bizseer.bigdata.metadataInfluxdb.DalServerException;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;

import java.util.Objects;

/**
 * @author zhangyingjie
 */
@Slf4j
public class HttpUtils {
    private static final MediaType JSON_TYPE =
            MediaType.Companion.parse("application/json;charset=utf-8");
    private final static OkHttpClient CLIENT = new OkHttpClient();

    public static String post(String url, String context) {
        Request request = new Request.Builder().url(url).post(RequestBody.create(context, JSON_TYPE)).build();
        try (Response response = CLIENT.newCall(request).execute()) {
            return Objects.requireNonNull(response.body()).string();
        } catch (Exception e) {
            log.error("调用dal的保存接口的时候，发生了异常url = {}，异常信息为：", url, e);
            throw new DalServerException(e.getMessage());
        }
    }
}