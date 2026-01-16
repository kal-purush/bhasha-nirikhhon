package top.spencer.crabscore.model.model.administrator;

import com.alibaba.fastjson.JSONObject;
import top.spencer.crabscore.base.BaseModel;
import top.spencer.crabscore.base.MyCallback;
import top.spencer.crabscore.common.CommonConstant;

import java.util.HashMap;
import java.util.Map;

/**
 * @author : spencercjh
 * @date : 2019/1/17 0:52
 */
public class OutputExcelModel extends BaseModel {
    /**
     * 参数表Integer competitionId String jwt
     *
     * @param myCallBack myCallBack
     */
    @Override
    public void execute(MyCallback<JSONObject> myCallBack) {
        String url = CommonConstant.URL + "admin/result/excel";
        Map<String, Object> postParams = new HashMap<>(4);
        postParams.put("competitionId", mvpParams[0]);
        requestPostAPI(url, postParams, myCallBack, mvpParams[1]);
    }
}