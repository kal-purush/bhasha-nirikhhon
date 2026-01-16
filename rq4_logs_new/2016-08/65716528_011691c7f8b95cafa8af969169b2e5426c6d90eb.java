package pf.xqq.unvideo;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by AXTing on 2016/4/17.
 */
public class GetDatas {

    public static List<User> getVideoInfo() {
        List<User> listDatas = new ArrayList<User>();

        listDatas.add(new User("flv","http://192.168.2.100/FLE/002186.flv"));
        listDatas.add(new User("动漫视频","http://clips.vorwaerts-gmbh.de/big_buck_bunny.mp4"));
        listDatas.add(new User("财富视频","http://newflv.jrj.com.cn/dby/20141017cjxsy.mp4"));
        listDatas.add(new User("光伏视频","http://180.212.82.145:18080/learning/sites/default/files/res2/u467/xue_xi_qing_jing_er_2.3guang_neng_de_zhuan_huan_xi_shou_yuan_li__0.mp4"));
        listDatas.add(new User("制冷视频","http://218.13.33.159:8000/rms/sites/default/files/mat_resource/mp4/kc17yl-wk10001.mp4"));
        listDatas.add(new User("微电子1","http://113.204.144.130/zyk/sites/default/files/file_dir/jcdlfzgy/g0103w0101.mp4"));
        listDatas.add(new User("微电子2","http://113.204.144.130/zyk/sites/default/files/file_dir/jcdlfzgy/g0103w0102.mp4"));
        listDatas.add(new User("微电子3","http://113.204.144.130/zyk/sites/default/files/file_dir/jcdlfzgy/g0103w0103.mp4"));
        listDatas.add(new User("微电子4","http://113.204.144.130/zyk/sites/default/files/file_dir/jcdlfzgy/g0103w0104.mp4"));
        listDatas.add(new User("微电子5","http://113.204.144.130/zyk/sites/default/files/file_dir/jcdlfzgy/g0103w0105.mp4"));
        listDatas.add(new User("微电子6","http://113.204.144.130/zyk/sites/default/files/file_dir/jcdlfzgy/g0103w0106.mp4"));




        return listDatas;
    }
}