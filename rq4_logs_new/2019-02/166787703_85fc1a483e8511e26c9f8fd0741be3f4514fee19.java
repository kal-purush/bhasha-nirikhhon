package kcl.paramount.group.business;

import kcl.paramount.group.dao.CatalogDao;
import kcl.paramount.group.dao.CatalogDaoJDBCImpl;
import kcl.paramount.group.util.JSONUtils;

import java.util.ArrayList;
import java.util.List;

public class CatalogBusiness {

    public String getCatalog(String username, String url) {
        String result = null;
        if (url != null && url.startsWith("/") == false) {
            url = "/" + url;
        }
        CatalogDao cd = new CatalogDaoJDBCImpl();
        List<String> list = new ArrayList<>();
        list = cd.getCatalog(username, url);
        list.addAll(cd.getFile(username, url));
        result = JSONUtils.getJSONString("success", list.toString());
        return result;
    }

}