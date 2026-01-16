package com.project.menus.vo;

public class MenuVo {

    private String menu_id;
    private String menu_name;

    public MenuVo(){}
    public MenuVo(String menu_id, String menu_name) {
        this.menu_id = menu_id;
        this.menu_name = menu_name;
    }

    public String getMenu_id() {
        return menu_id;
    }

    public void setMenu_id(String menu_id) {
        this.menu_id = menu_id;
    }

    public String getMenu_name() {
        return menu_name;
    }

    public void setMenu_name(String menu_name) {
        this.menu_name = menu_name;
    }

    @Override
    public String toString() {
        return "MenusVo{" +
                "menu_id='" + menu_id + '\'' +
                ", menu_name='" + menu_name + '\'' +
                '}';
    }
}