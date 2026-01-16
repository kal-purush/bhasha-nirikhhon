package com.quickcanteen.quickcanteen.actions.menu.impl;

import android.app.Activity;
import com.quickcanteen.quickcanteen.actions.BaseActionImpl;
import com.quickcanteen.quickcanteen.actions.menu.IMenuAction;
import com.quickcanteen.quickcanteen.utils.BaseJson;
import com.quickcanteen.quickcanteen.utils.HttpUtils;
import org.json.JSONException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by 11022 on 2017/6/30.
 */
public class MenuActionImpl extends BaseActionImpl implements IMenuAction {
    public MenuActionImpl(Activity activity) {
        super(activity);
    }

    @Override
    public BaseJson getTypesAndDishesByCompanyId(int companyId) throws IOException,JSONException{
        Map<String, String> map = new HashMap<>();
        companyId = 1;
        map.put("companyId", String.valueOf(companyId));
        String result = HttpUtils.httpConnectByPost("menu/getTypesAndDishesByCompanyId", map);
        return new BaseJson(result);
    }
}