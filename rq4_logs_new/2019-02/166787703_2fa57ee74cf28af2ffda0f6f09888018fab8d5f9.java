package kcl.paramount.group.business;

import kcl.paramount.group.dao.CatalogDetailDao;
import kcl.paramount.group.dao.CatalogDetailDaoJDBCImpl;
import kcl.paramount.group.util.JSONUtils;

import java.util.ArrayList;
import java.util.List;

public class CatalogDetailBusiness {

    public String getDetailDirs(String username, String url) {
        String result = null;
        if (url != null && url.startsWith("/") == false) {
            url = "/" + url;
        }
        CatalogDetailDao cd = new CatalogDetailDaoJDBCImpl();
        List<String> list = new ArrayList<>();
        list = cd.getDirs(username, url);
        result = JSONUtils.getJSONString("success", list.toString());
        return result;
    }

    public String getDetailFiles(String username, String url) {
        String result = null;
        if (url != null && url.startsWith("/") == false) {
            url = "/" + url;
        }
        CatalogDetailDao cd = new CatalogDetailDaoJDBCImpl();
        List<String> list = new ArrayList<>();
        list = cd.getFiles(username, url);
        result = JSONUtils.getJSONString("success", list.toString());
        return result;
    }

}