package com.liang.JsonUtil;

import com.alibaba.fastjson.JSON;
import com.liang.model.Flight;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;

public  class FastJsonUtil {

    public static void sendJsonData(HttpServletResponse response, Object data)
            throws Exception
    {
        response.setContentType("text/html;charset=UTF-8");
        PrintWriter out = response.getWriter();
        if (data instanceof Flight){
          out.print(JSON.toJSONString(data));
        }
        else if (data instanceof String){
            out.println(data);
        }
        else if (data instanceof Integer){
            out.print(JSON.toJSONString(data));
        }
        else {
            out.print(JSON.toJSONString(data));
        }
        out.flush();
        out.close();
    }
}
