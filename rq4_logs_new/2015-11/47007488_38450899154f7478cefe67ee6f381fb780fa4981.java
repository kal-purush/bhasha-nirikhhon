package com.posmanagement.webui;

import com.posmanagement.utils.DbManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class UserMenu {
    public UserMenu(int userID) {
        userID_ = userID;
    }

    public String generateHTMLString() throws Exception {
        return generateMainMenu(0);
    }

    private  String generateMainMenu(int pid) throws Exception {
        ArrayList<HashMap<String, Object>> dbRet = fetchMenuByPid(pid);
        if (dbRet.size() <= 0)
            return new String("");

        String htmlString = "";
        for (int index = 0; index < dbRet.size(); ++index) {
            String subMenu = generateSubMenu(Integer.parseInt(dbRet.get(index).get("ID").toString()));
            if (subMenu.length() != 0) {
                htmlString += generateMainMenuHtml(dbRet.get(index).get("ID").toString(),
                        dbRet.get(index).get("MENUNAME").toString(),
                        subMenu);
            }
        }
        return htmlString;
    }

    private String generateSubMenu(int pid) throws Exception {
        ArrayList<HashMap<String, Object>> dbRet = fetchMenuByPid(pid);
        if (dbRet.size() <= 0)
            return new String("");

        String htmlString = "";
        for (int index = 0; index < dbRet.size(); ++index) {
            // TODO for layer3
            // generateSubMenu()
            htmlString += generateSubMenuHtml(dbRet.get(index).get("WEBPATH").toString(), dbRet.get(index).get("MENUNAME").toString());
        }
        return htmlString;
    }

    private String generateMainMenuHtml(String menuID, String menuName, String subMenu) {
        String htmlString = new String();
        htmlString += "<dl id=\"1menu-article" + menuID + "\" >";
        htmlString += "<dt ><i class=\"Hui-iconfont\" >&#xe616;</i> "
                + menuName
                + "<i class=\"Hui-iconfont menu_dropdown-arrow\">&#xe6d5;</i></dt>";
        htmlString += "<dd><ul>";
        htmlString += subMenu;
        htmlString += "</dd></ul></dl>";
        return htmlString;
    }

    private  String generateSubMenuHtml(String webPath, String menuName) {
        String htmlString = new String();
        htmlString += "<li><a _href=\"";
        if (webPath.length() == 0)
            htmlString += "404.html";
        else
            htmlString += webPath;
        htmlString += "\" href=\"javascript:void(0)\">";
        htmlString += menuName;
        htmlString += "</a></li>";
        return htmlString;
    }

    private ArrayList<HashMap<String, Object>> fetchMenuByPid(int pid) throws Exception {
        Map parametMap = new HashMap();
        ArrayList<HashMap<String, Object>> dbRet  = null;
        parametMap.put(1,pid);
        return DbManager.getDbManager("").executeSql("select * from menutree where preid=? order by menuorder", (HashMap<Integer, Object>)parametMap);
    }

    private int userID_; // TODO for role
}