package com.treeexample.mb;

import com.treeexample.dao.TreeDao;
import com.treeexample.entity.Kategori;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.PostConstruct;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.RequestScoped;
import org.primefaces.model.menu.DefaultMenuItem;
import org.primefaces.model.menu.DefaultMenuModel;
import org.primefaces.model.menu.DefaultSubMenu;
import org.primefaces.model.menu.MenuModel;

@ManagedBean
@RequestScoped
public class MenuBean {

    private TreeDao treeDao = new TreeDao();
    private static List<Kategori> liste;
    private MenuModel model;
    private DefaultSubMenu subMenu;
    private String page;

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }
    
    

    @PostConstruct
    private void init() {

        page="anaSayfa.xhtml";
        liste = new ArrayList<>();
        model = new DefaultMenuModel();
        liste = treeDao.getirKategoriListesi(0);
        for (Kategori menu : liste) {

            if (menu.getAnaKatMi() == '1') {
                DefaultSubMenu sMenu = new DefaultSubMenu(menu.getKategoriAdi());
                subMenu = menuGetir(menu.getKategoriId(), sMenu);
            }
            model.addElement(subMenu);
        }
    }

    private DefaultSubMenu menuGetir(int id, DefaultSubMenu subMenu) {

        try {

            liste = treeDao.getirKategoriListesi(id);

            for (Kategori menu : liste) {

                if (menu.getAnaKatMi() == '1') {

                    DefaultSubMenu sMenu = new DefaultSubMenu(menu.getKategoriAdi());
                    this.menuGetir(menu.getKategoriId(), sMenu);

                    subMenu.addElement(sMenu);

                } else {

                    DefaultMenuItem item = new DefaultMenuItem(menu.getKategoriAdi());
                    item.setUrl(menu.getHedef()+"?faces-redirect=true");             
                    item.setUpdate("form:content");
                    item.setPartialSubmit(true);
                    subMenu.addElement(item);

                }

            }

        } catch (Exception e) {
        }
        return subMenu;
    }

    public static List<Kategori> getListe() {
        return liste;
    }

    public static void setListe(List<Kategori> liste) {
        MenuBean.liste = liste;
    }

    public MenuModel getModel() {
        return model;
    }

    public void setModel(MenuModel model) {
        this.model = model;
    }

    public DefaultSubMenu getSubMenu() {
        return subMenu;
    }

    public void setSubMenu(DefaultSubMenu subMenu) {
        this.subMenu = subMenu;
    }

    public TreeDao getTreeDao() {
        return treeDao;
    }

    public void setTreeDao(TreeDao treeDao) {
        this.treeDao = treeDao;
    }


}