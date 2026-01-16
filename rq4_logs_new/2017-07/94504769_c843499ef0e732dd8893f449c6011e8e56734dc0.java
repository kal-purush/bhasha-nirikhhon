package com.zxw.dispatch.recycler.viewHolder;

import android.app.Dialog;
import android.support.annotation.LayoutRes;
import android.text.TextUtils;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.LinearLayout;
import android.widget.TextView;

import com.jude.easyrecyclerview.adapter.BaseViewHolder;
import com.zxw.data.bean.DriverWorkloadItem;
import com.zxw.data.bean.LineParams;
import com.zxw.data.bean.SendHistory;
import com.zxw.dispatch.R;
import com.zxw.dispatch.presenter.BasePresenter;
import com.zxw.dispatch.presenter.MainPresenter;
import com.zxw.dispatch.utils.DisplayTimeUtil;
import com.zxw.dispatch.view.dialog.StartCarRemarkDialog;

import butterknife.Bind;

import static com.zxw.dispatch.R.id.tv_interval_time;

/**
 * Created by Thinkpad on 2017/7/7.
 */

public class GoneForNormalViewHolder extends BaseViewHolder<SendHistory> {
    TextView tvCarSequence;
    TextView tvCarCode;
    TextView tvPlanTime;
    TextView tvArriveTime;
    TextView tvIntervalTime;
    TextView tvSendTime;
    TextView tvDriver;
    TextView tvTrainman;
    TextView tvWorkStatus;
    TextView tv_driver_ok;
    TextView tv_send_remark;
    TextView tv_send_withdraw;
    TextView tv_stop_car_minute;

    private final LineParams mLineParams;
    private final LayoutInflater mLayoutInflater;
    private MainPresenter presenter;

    public GoneForNormalViewHolder(ViewGroup parent, LineParams mLineParams, MainPresenter presenter) {
        super(parent, R.layout.item_start_car_line_operate);
        tvCarSequence = $(R.id.tv_car_sequence);
        tvCarCode = $(R.id.tv_car_code);
        tvPlanTime = $(R.id.tv_plan_time);
        tvArriveTime = $(R.id.tv_arrive_time);
        tvIntervalTime = $(R.id.tv_interval_time);
        tvSendTime = $(R.id.tv_send_time);
        tvDriver = $(R.id.tv_driver);
        tvTrainman = $(R.id.tv_trainman);
        tvWorkStatus = $(R.id.tv_work_status);
        tv_driver_ok = $(R.id.tv_driver_ok);
        tv_send_remark = $(R.id.tv_send_remark);
        tv_send_withdraw = $(R.id.tv_send_withdraw);
        tv_stop_car_minute = $(R.id.tv_stop_car_minute);
        mLayoutInflater = LayoutInflater.from(getContext());
        this.mLineParams = mLineParams;
        this.presenter = presenter;
    }

    @Override
    public void setData(final SendHistory data) {
        super.setData(data);
        tvCarSequence.setText(getDataPosition() + 1 + "");
        tvCarCode.setText(data.code);
        if (data.taskEditBelongId == data.taskEditRunId){
            tvCarCode.setBackground(getContext().getResources().getDrawable(R.drawable.ll_stop_car_red_btn_bg));
        }else {
            tvCarCode.setBackground(getContext().getResources().getDrawable(R.drawable.ll_stop_car_green_btn_bg));
        }
        tvDriver.setText(data.driverName);
        if (data.stewardName != null && !TextUtils.isEmpty(data.stewardName))
            tvTrainman.setText(data.stewardName);
        else
            tvTrainman.setText("");

        String spaceTime = data.spaceTime;
        if (TextUtils.isEmpty(spaceTime)){
            tvIntervalTime.setText("首班");
        }else{
            tvIntervalTime.setText(spaceTime);
        }

        tvPlanTime.setText(DisplayTimeUtil.substring(data.vehTime));
        // 到站时刻
        if (data.arriveTime != null && !TextUtils.isEmpty(data.arriveTime))
            tvArriveTime.setText(DisplayTimeUtil.substring(data.arriveTime));
        else
            tvArriveTime.setText("");
        // 实际发车时刻
        if (data.vehTimeReal != null && !TextUtils.isEmpty(data.vehTimeReal))
            tvSendTime.setText(DisplayTimeUtil.substring(data.vehTimeReal));
        else
            tvSendTime.setText("");
        // 停场时间
        tv_stop_car_minute.setText(setStopCarMinute(data));


        tvWorkStatus.setText(data.typeName);
        if (mLineParams.getSaleType() == MainPresenter.TYPE_SALE_AUTO){
            tvTrainman.setVisibility(View.GONE);
        }else if(mLineParams.getSaleType() == MainPresenter.TYPE_SALE_MANUAL){
            tvTrainman.setVisibility(View.VISIBLE);
        }
        //1正常、2异常
        if (data.status == 1){
            tv_send_remark.setTextColor(getContext().getResources().getColor(R.color.font_blue2));
        }else{
            tv_send_remark.setTextColor(getContext().getResources().getColor(R.color.font_gray));
        }
        // 备注
        tv_send_remark.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                new StartCarRemarkDialog(getContext(), data.id, data.status, data.remarks,
                        new StartCarRemarkDialog.OnStartCarRemarkListener() {
                            @Override
                            public void goneCarNormalRemarks(int objId, int status, String remarks, BasePresenter.LoadDataStatus loadDataStatus) {
                                presenter.goneCarNormalRemarks(objId, status, remarks, loadDataStatus);
                            }

                            @Override
                            public void goneCarAbNormalRemarks(int objId, int status, String remarks,
                                                               int runOnce, double runMileage, double runEmpMileage, BasePresenter.LoadDataStatus loadDataStatus) {
                                presenter.goneCarAbNormalRemarks(objId,status,remarks,runOnce,runMileage,runEmpMileage, loadDataStatus);
                            }
                        });
            }
        });

        String driverStatus = "";
        if (data.opStatus !=null){
            switch (data.opStatus){
                case 1:
                    driverStatus = "待开始";
                    break;
                case 2:
                    driverStatus = "进行中";
                    break;
                case 3:
                    driverStatus = "异常终止";
                    break;
                case 4:
                    driverStatus = "正常结束";
                    break;
            }
        }
        tv_driver_ok.setText(driverStatus);

        // 撤回
        // 如果已有实际发车时间, 则因此撤回按钮
        if(TextUtils.isEmpty(data.vehTimeReal)){
            tv_send_withdraw.setVisibility(View.VISIBLE);
        }else{
            tv_send_withdraw.setVisibility(View.GONE);
        }
        tv_send_withdraw.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                openWithdrawCarDialog(data.id);
            }
        });

    }

    private String setStopCarMinute(SendHistory history){
        Long arriveMinute;
        Long sendMinute = null;
        if (history.arriveTime != null && !TextUtils.isEmpty(history.arriveTime)){
            arriveMinute = Long.valueOf(history.arriveTime.substring(0,2)) * 60
                    + Long.valueOf(history.arriveTime.substring(2,4));
        }else{
            return "";
        }
        if (history.vehTimeReal != null && !TextUtils.isEmpty(history.vehTimeReal)){
            sendMinute = Long.valueOf(history.vehTimeReal.substring(0,2)) * 60
                    + Long.valueOf(history.vehTimeReal.substring(2,4));
        }else{
            return "";
        }
        Long min = (Long)sendMinute - arriveMinute;
        if (min >= 0) {
            return String.valueOf(min);
        }else{
            return "";
        }

    }


    /**
     * 查看
     */
    private void openCarMsgDialog() {
        final Dialog mDialog = new Dialog(getContext(),R.style.customDialog);
        LinearLayout.LayoutParams params = new LinearLayout.LayoutParams(LinearLayout.LayoutParams.WRAP_CONTENT,
                LinearLayout.LayoutParams.WRAP_CONTENT);
        View view = View.inflate(getContext(),R.layout.view_check_car_details_dialog,null);
        Button btn_close = (Button) view.findViewById(R.id.btn_close);
        btn_close.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                mDialog.dismiss();
            }
        });
        mDialog.setContentView(view,params);
        mDialog.setCancelable(true);
        mDialog.show();
    }

    /**
     * 撤回
     */
    private void openWithdrawCarDialog(final int objId) {
        final Dialog mDialog = new Dialog(getContext(),R.style.customDialog);
        LinearLayout.LayoutParams params = new LinearLayout.LayoutParams(LinearLayout.LayoutParams.WRAP_CONTENT,
                LinearLayout.LayoutParams.WRAP_CONTENT);
        View view = View.inflate(getContext(),R.layout.view_withdraw_dialog,null);
        TextView tv_prompt = (TextView) view.findViewById(R.id.tv_prompt);
        tv_prompt.setText("您确定把车辆撤回到待发车辆列表？");
        final Button btn_confirm = (Button) view.findViewById(R.id.btn_confirm);
        Button btn_cancel = (Button) view.findViewById(R.id.btn_cancel);
        btn_confirm.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                btn_confirm.setClickable(false);
                presenter.callBackGoneCar(objId, new BasePresenter.LoadDataStatus() {
                    @Override
                    public void OnLoadDataFinish() {
                        mDialog.dismiss();
                    }
                });

            }
        });
        btn_cancel.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                mDialog.dismiss();
            }
        });
        mDialog.setContentView(view,params);
        mDialog.setCancelable(true);
        mDialog.show();
    }
}