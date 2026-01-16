package com.seepine.http;

import com.seepine.http.entity.Request;
import com.seepine.http.entity.StreamProgress;
import com.seepine.http.util.HttpUtil;
import com.seepine.http.util.StrUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Seepine
 */
public class Main {
    public static void main(String[] args) throws IOException {
        testDownload();
        testGet();
        testPost();
        testPut();
        testDelete();
    }

    /**
     * test get
     */
    public static void testGet() throws IOException {
        System.out.println("Start Test Get:");
        System.out.println("test err:" + StrUtil.CRLF + HttpUtil.get("http://127.0.0.1:8080/get/restful"));
        System.out.println("test get:" + StrUtil.CRLF + HttpUtil.get("http://127.0.0.1:8080/get"));
        System.out.println("test restful:" + StrUtil.CRLF + HttpUtil.get("http://127.0.0.1:8080/get/restful/561561561"));
        System.out.println("test param:" + StrUtil.CRLF + HttpUtil.get("http://127.0.0.1:8080/get/param?name=mfoinawonfw"));
        System.out.println("test param2:" + StrUtil.CRLF + HttpUtil.get("http://127.0.0.1:8080/get/param2?id=3124421&name=43关闭你"));
        Map<String, Object> params = new HashMap<>(2);
        params.put("id", 164981165);
        params.put("name", "个去驲5如125rf给");
        System.out.println("test param3:" + StrUtil.CRLF + HttpUtil.get("http://127.0.0.1:8080/get/param2", params));
        System.out.println("test redirect:" + StrUtil.CRLF + HttpUtil.execute(Request.build("http://127.0.0.1:8080/get/redirect")).getBody());
        System.out.println("test not auto redirect:" + StrUtil.CRLF + HttpUtil.execute(Request.build("http://127.0.0.1:8080/get/redirect").redirect(false)).getLocation());
    }

    /**
     * test post
     */
    public static void testPost() throws IOException {
        System.out.println("Start Test Post:");
        System.out.println("test err:" + StrUtil.CRLF + HttpUtil.post("http://127.0.0.1:8080/post/restful"));
        System.out.println("test post:" + StrUtil.CRLF + HttpUtil.post("http://127.0.0.1:8080/post"));
        System.out.println("test param:" + StrUtil.CRLF + HttpUtil.post("http://127.0.0.1:8080/post/param?name=mfoinawonfw"));
        System.out.println("test param2:" + StrUtil.CRLF + HttpUtil.post("http://127.0.0.1:8080/post/param2?id=3124421&name=43关闭你"));
        Map<String, Object> params = new HashMap<>(2);
        params.put("id", 164981165);
        params.put("name", "个去驲5如125rf给");
        System.out.println("test param2 by map:" + StrUtil.CRLF + HttpUtil.post("http://127.0.0.1:8080/post/param2", params));
        System.out.println("test json:" + StrUtil.CRLF + HttpUtil.postJson("http://127.0.0.1:8080/post/json", params));
    }

    /**
     * test put
     */
    public static void testPut() throws IOException {
        System.out.println("Start Test Put:");
        System.out.println("test err:" + StrUtil.CRLF + HttpUtil.put("http://127.0.0.1:8080/put/restful"));
        System.out.println("test post:" + StrUtil.CRLF + HttpUtil.put("http://127.0.0.1:8080/put"));
        System.out.println("test param:" + StrUtil.CRLF + HttpUtil.put("http://127.0.0.1:8080/put/param?name=mfoinawonfw"));
        System.out.println("test param2:" + StrUtil.CRLF + HttpUtil.put("http://127.0.0.1:8080/put/param2?id=3124421&name=43关闭你"));
        Map<String, Object> params = new HashMap<>(2);
        params.put("id", 164981165);
        params.put("name", "个去驲5如125rf给");
        System.out.println("test param2 by map:" + StrUtil.CRLF + HttpUtil.put("http://127.0.0.1:8080/put/param2", params));
        System.out.println("test json:" + StrUtil.CRLF + HttpUtil.putJson("http://127.0.0.1:8080/put/json", params));
    }

    /**
     * test delete
     */
    public static void testDelete() throws IOException {
        System.out.println();
        System.out.println("Start Test Delete:");
        System.out.println("test err:" + StrUtil.CRLF + HttpUtil.delete("http://127.0.0.1:8080/delete/restful"));
        System.out.println("test post:" + StrUtil.CRLF + HttpUtil.delete("http://127.0.0.1:8080/delete"));
        System.out.println("test param:" + StrUtil.CRLF + HttpUtil.delete("http://127.0.0.1:8080/delete/param?name=mfoinawonfw"));
        System.out.println("test param2:" + StrUtil.CRLF + HttpUtil.delete("http://127.0.0.1:8080/delete/param2?id=3124421&name=43关闭你"));
        Map<String, Object> params = new HashMap<>(2);
        params.put("id", 164981165);
        params.put("name", "个去驲5如125rf给");
        System.out.println("test param2 by map:" + StrUtil.CRLF + HttpUtil.delete("http://127.0.0.1:8080/delete/param2", params));
        System.out.println("test json:" + StrUtil.CRLF + HttpUtil.deleteJson("http://127.0.0.1:8080/delete/json", params));
    }

    public static void testDownload() throws FileNotFoundException {
        FileOutputStream fileOutputStream = new FileOutputStream(new File("E:\\b.mp4"));
        try {
            HttpUtil.download("https://txmov2.a.yximgs.com/upic/2020/08/07/21/BMjAyMDA4MDcyMTU2MTZfMTQyNDg2ODk1MF8zMzkwMDU3MDc1Nl8xXzM=_b_B4d8c7235f8c75fe7ab74fe9016bfa327.mp4"
                    , fileOutputStream, new StreamProgress() {
                        private long contentLength = 0;

                        @Override
                        public void startBefore(long contentLength) {
                            System.out.println("开始下载：" + contentLength);
                            this.contentLength = contentLength;
                        }

                        @Override
                        public void start() {
                            System.out.println("开始");
                        }

                        @Override
                        public void progress(long progressSize) {
                            System.out.println("进度：" + progressSize * 1.0 / contentLength * 100);
                        }

                        @Override
                        public void finish() {
                            System.out.println("下载完成");
                        }
                    });
        } catch (UnknownHostException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}