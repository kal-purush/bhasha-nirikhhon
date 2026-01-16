package wangfeixixi.cip.ui.MapSimple;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;

import wangfeixixi.cip.R;
import wangfeixixi.cip.beans.JsonRootBean;
import wangfeixixi.cip.fram.BaseActivity;
import wangfeixixi.cip.ui.NumberTransfer;
import wangfeixixi.cip.ui.bird.BirdActivity;
import wangfeixixi.cip.utils.VertionUtils;
import wangfeixixi.cip.utils.WifiUtils;
import wangfeixixi.cip.utils.date.DateUtils;

public class MapSimpleActivity extends BaseActivity<MapSimpleDelegate> implements View.OnClickListener, View.OnLongClickListener {
    @Override
    protected Class<MapSimpleDelegate> getDelegateClass() {
        return MapSimpleDelegate.class;
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        viewDelegate.onCreate(savedInstanceState);
    }

    @Override
    protected void onResume() {
        super.onResume();
        viewDelegate.onResume();

        viewDelegate.setOnClickListener(this, R.id.btn_jump);
        viewDelegate.setOnLongClickListener(this, R.id.btn_jump);
    }

    @Override
    public void onClick(View v) {
        viewDelegate.showLogView();
    }

    @Override
    public boolean onLongClick(View v) {
        startActivity(new Intent(this, BirdActivity.class));
        return true;
    }

    @Override
    protected void onSaveInstanceState(Bundle outState) {
        super.onSaveInstanceState(outState);
        viewDelegate.onSaveInstanceState(outState);
    }

    @Override
    protected void onPause() {
        super.onPause();
        viewDelegate.onPause();
    }

    @Override
    protected void onDestroy() {
        viewDelegate.onDestroy();
        super.onDestroy();
    }

    private int lastHeading = 0;

    @Override
    protected void onReceiveJsonBean(JsonRootBean bean) {
        if (bean.hvDatas == null || bean.rvDatas == null) {
            viewDelegate.setLogText(bean.toString());
            return;
        }
        //显示log
        addLog(bean);
        viewDelegate.setSpeed(NumberTransfer.double2String(bean.hvDatas.speed * 3.6f));
        viewDelegate.setDistance(NumberTransfer.double2String(bean.rvDatas.get(0).distance));


        if (++lastHeading == 5) {
            viewDelegate.lbsChangeBearing(bean.hvDatas.heading);//旋转角度
            lastHeading = 0;
        }
        //更新地图位置
        viewDelegate.lbsAddBenMaker(bean.hvDatas);
        for (int i = 0; i < bean.rvDatas.size(); i++)
            viewDelegate.lbsAddOtherMaker(bean.rvDatas.get(i));
        viewDelegate.switchCapionHeight(bean.hvDatas.cw != 0);
        viewDelegate.switchCW(bean.hvDatas.direction);
    }

    private void addLog(JsonRootBean bean) {
        StringBuffer sb = new StringBuffer();
        String wifiName = WifiUtils.getWifiName();
        sb.append("\nwifiName:" + wifiName);
        sb.append("\n版本：" + VertionUtils.getVersionName() + "-" + VertionUtils.getVersionCode());
        sb.append("\n日期：" + String.valueOf(DateUtils.getCurrentDate(DateUtils.dateFormatYMDHMS)));
        float distance = viewDelegate.getLbsDistance(bean.hvDatas.latitude, bean.hvDatas.longitude, bean.rvDatas.get(0).latitude, bean.rvDatas.get(0).longitude);
        sb.append("\nmap距离：" + NumberTransfer.double2String(distance) + " m");
        sb.append("\ndistance：" + NumberTransfer.double2String(bean.rvDatas.get(0).distance) + " m");
        sb.append("\n距离差值：" + (NumberTransfer.double2String(distance - bean.rvDatas.get(0).distance) + " m"));
//        double jvli = 0;
//        float mixDiagonal = 0;
//        if (bean.rvDatas != null && bean.rvDatas.size() > 0) {
//            mixDiagonal = CarUtils.getInstance().getMixDiagonal(bean.rvDatas.get(0).x, bean.rvDatas.get(0).y);
//            double sqrt = Math.sqrt(Math.abs(bean.rvDatas.get(0).x) * Math.abs(bean.rvDatas.get(0).x) + Math.abs(bean.rvDatas.get(0).y) * Math.abs(bean.rvDatas.get(0).y));
//            sqrt -= mixDiagonal;
//            jvli = Double.parseDouble(new DecimalFormat("#.##").format(sqrt));
//        }
//        if (jvli >= mixDiagonal) {
//            sb.append("\n微调距离:" + (jvli - mixDiagonal));
//        } else {
//            sb.append("\n微调距离:" + 0);
//        }

        sb.append(bean.toString());
        viewDelegate.setLogText(sb.toString());
    }


}