package flyweight.structcode;

/**
 * Created by fisbii on 16-5-18.
 */
public class Client {
    public static void main(String args[]){
        WebSiteFactory webSiteFactory = new WebSiteFactory();

        WebSite fx = webSiteFactory.getWebSiteCategory("产品展示");
        fx.use(new User("小菜"));

        WebSite fy = webSiteFactory.getWebSiteCategory("产品展示");
        fy.use(new User("大鸟"));

        WebSite fz = webSiteFactory.getWebSiteCategory("产品展示");
        fy.use(new User("娇娇"));

        WebSite fl = webSiteFactory.getWebSiteCategory("博客");
        fy.use(new User("老顽童"));

        WebSite fm = webSiteFactory.getWebSiteCategory("博客");
        fy.use(new User("桃谷六仙"));

        WebSite fn = webSiteFactory.getWebSiteCategory("博客");
        fy.use(new User("南海鳄神"));

        System.out.println("得到网站分类总数为：" + webSiteFactory.getWebSiteCount());
    }
}