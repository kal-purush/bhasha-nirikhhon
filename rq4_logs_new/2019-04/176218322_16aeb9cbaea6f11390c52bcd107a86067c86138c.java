package com.example.lui_project;

import android.app.DatePickerDialog;
import android.content.Intent;
import android.util.Log;
import android.view.KeyEvent;
import android.view.View;
import android.widget.Button;
import android.widget.DatePicker;
import android.widget.TextView;
import android.widget.Toast;

import com.example.lui_project.base.BaseActivity;
import com.example.lui_project.utils.Constant;
import com.example.lui_project.utils.GetBMIValuesHelper;
import com.example.lui_project.utils.SaveKeyValues;
import mrkj.library.wheelview.scalerulerview.ScaleRulerView;

import java.util.Calendar;
import java.util.Map;


/**
 * 制定计划-->设置相关信息
 */
public class PlanningActivity extends BaseActivity implements View.OnFocusChangeListener, View.OnClickListener{
    private static final int SET_START_DATE = 0;//設置開始時間
    private static final int SET_STOP_DATE = 1;//設置結束時間
    // 功能
    private DatePickerDialog datePickerDialog;//創建時間選擇器
    private GetBMIValuesHelper getBMIValuesHelper;//獲取體指信息
    //控件
    private TextView setStartTime,setStopTime;//設置開始和結束的時間
    private TextView show_plan_weight;//顯示期望體重
    private ScaleRulerView setPlanWeight;//設置目標體重
    private Button finish;//完成按鈕
    private TextView capion;//標準體重範圍提示
    private TextView hint;//提示
    // 數值
    private boolean isSetStart;//區別設置
    private int nowYear;//當前年
    private int nowMonth;//當前月
    private int nowDate;//當前日
    private Double min_normal_weight;//標準體重的最小值
    private Double max_normal_weight;//標準體重的最大值
    private int now_weight;//當前體重
    private int start_year;//始年
    private int start_month;//開始月
    private int start_date;//開始日
    private int stop_year;//結束年
    private int stop_month;//結束月
    private int stop_date;//結束日
    private int plan_want_weight;//目標體重
    /* 設置標題*/
    @Override
    protected void setActivityTitle() {
        initTitle();//init title
        setTitle(getString(R.string.target));//set title
        setTitleTextColor(getResources().getColor(R.color.black));//set text color
    }

    /**
     * init layer
     */
    @Override
    protected void getLayoutToView() {
        setContentView(R.layout.activity_planning);
    }

    /**
     * init related variable
     */
    @Override
    protected void initValues() {
        //設置默認加載find page
        SaveKeyValues.putIntValues("launch_which_fragment", Constant.MAKE_PLAN);
        getNowDate();
        int height = SaveKeyValues.getIntValues("height",0);
        int weight = SaveKeyValues.getIntValues("weight",0);
        Log.e("身高體重值","身高："+height + "\t\t體重："+weight);
        getBMIValuesHelper = new GetBMIValuesHelper();
        Map<String,Double> map = getBMIValuesHelper.getNormalWeightRange(height);
        min_normal_weight = map.get("min");
        max_normal_weight = map.get("max");
        now_weight = weight;
    }

    /**
     * 目前日期
     */
    private void getNowDate(){
        Calendar calendar = Calendar.getInstance();
        nowYear = calendar.get(Calendar.YEAR);
        nowMonth = calendar.get(Calendar.MONTH);
        nowDate = calendar.get(Calendar.DAY_OF_MONTH);
    }
    /**
     * init control
     */
    @Override
    protected void initViews() {
        hint = (TextView) findViewById(R.id.change_txt);
        setStartTime = (TextView) findViewById(R.id.plan_start_time);
        setStopTime = (TextView) findViewById(R.id.plan_stop_time);
        setPlanWeight = (ScaleRulerView) findViewById(R.id.plan_input_weight);
        show_plan_weight = (TextView) findViewById(R.id.plan_show_weight);
        capion = (TextView) findViewById(R.id.show_normal_weight_range);
        finish = (Button) findViewById(R.id.finish);

    }

    /**
     * 設置ruler view
     */
    private ScaleRulerView.OnValueChangeListener set_plan_weight_listener = new ScaleRulerView.OnValueChangeListener() {
        @Override
        public void onValueChange(float value) {
            show_plan_weight.setText((int)value + getString(R.string.kg));
            plan_want_weight = (int) value;
            if ((int)value != now_weight){
                hint.setText(getString(R.string.plan_weight));
            }
        }
    };

    /**
     * monitor
     */
    @Override
    protected void setViewsListener() {
        setStartTime.setOnFocusChangeListener(this);
        setStopTime.setOnFocusChangeListener(this);
        setStartTime.setOnClickListener(this);
        setStopTime.setOnClickListener(this);
        finish.setOnClickListener(this);
        setPlanWeight.setValueChangeListener(set_plan_weight_listener);
    }

    /**
     * set function
     */
    @Override
    protected void setViewsFunction() {
        setStartTime.setClickable(true);
        setStartTime.setFocusableInTouchMode(true);
        setStopTime.setClickable(true);
        setStopTime.setFocusableInTouchMode(true);
        capion.setText(getString(R.string.normal_weight_range) + min_normal_weight + "~" + max_normal_weight + getString(R.string.kg));
        //target height selector
        setPlanWeight.initViewParam(now_weight, 130, 30);
        //create time selector
        datePickerDialog = new DatePickerDialog(this, new DatePickerDialog.OnDateSetListener() {
            @Override
            public void onDateSet(DatePicker view, int year, int monthOfYear, int dayOfMonth) {

                if (isSetStart){//set start time
                    start_year = year;
                    start_month = monthOfYear + 1;
                    start_date = dayOfMonth;
                    setStartTime.setText(year+getString(R.string.year)+(monthOfYear+1)+getString(R.string.month)+dayOfMonth+getString(R.string.day));
                }else {//set end time
                    stop_year = year;
                    stop_month = monthOfYear + 1;
                    stop_date = dayOfMonth;
                    setStopTime.setText(year+getString(R.string.year)+(monthOfYear+1)+getString(R.string.month)+dayOfMonth+getString(R.string.day));
                }
            }
        },nowYear,nowMonth,nowDate);
    }

    /**
     * 焦點
     * @param v
     * @param hasFocus
     */
    @Override
    public void onFocusChange(View v, boolean hasFocus) {
        switch (v.getId()){
            case R.id.plan_start_time://計畫開始時間
                if (hasFocus){
                    isSetStart = true;
                    showDateDialog(SET_START_DATE);
                }
                break;
            case R.id.plan_stop_time://計畫結束時間
                if (hasFocus){
                    isSetStart = false;
                    showDateDialog(SET_STOP_DATE);
                }
                break;
            default:
                break;
        }
    }

    /**
     * 顯示選擇日期時間彈窗
     * @param type
     */
    private void showDateDialog(int type){

        if (type == SET_START_DATE){
            datePickerDialog.setTitle(getString(R.string.set_start_time));
            datePickerDialog.show();
        }else {
            datePickerDialog.setTitle(getString(R.string.set_stop_time));
            datePickerDialog.show();
        }
    }
    /**
     * click event
     * @param v
     */
    @Override
    public void onClick(View v) {
        switch (v.getId()){
            case R.id.plan_start_time:
                showDateDialog(SET_START_DATE);
                break;
            case R.id.plan_stop_time:
                showDateDialog(SET_STOP_DATE);
                break;
            case R.id.finish://完成
                String start_dates = setStartTime.getText().toString();
                String stop_dates = setStopTime.getText().toString();
                if (plan_want_weight < min_normal_weight){
                    Toast.makeText(this,getString(R.string.welceom_plan_error1), Toast.LENGTH_SHORT).show();
                }else if(plan_want_weight <= max_normal_weight){
                    if ((!getString(R.string.set_start_time).equals(start_dates))&&(!getString(R.string.set_stop_time).equals(stop_dates))){
                       if ((start_date <= stop_date) && (start_year <= stop_year) && (start_month <= stop_month)){
                           saveCustomSettingValues(start_dates,stop_dates);
                           Intent intent = new Intent(this,FunctionActivity.class);
                           startActivity(intent);
                           finish();
                       }else {
                           Toast.makeText(this,getString(R.string.welceom_plan_error2), Toast.LENGTH_SHORT).show();
                       }
                    }else {
                        Toast.makeText(this,getString(R.string.welceom_plan_error3), Toast.LENGTH_SHORT).show();
                    }
                }else{
                    Toast.makeText(this,getString(R.string.welceom_plan_error4), Toast.LENGTH_SHORT).show();
                }
                break;
            default:
                break;
        }
    }

    /**
     * 儲存用戶的相關信息*/
    //1、儲存開始日期年月日
    // 2、儲存結束日期年月日
    // 3、儲存體重範圍值標準最小值和標準最大值
    // 4、儲存目標體重

    private void saveCustomSettingValues(String start_dates, String stop_dates){
        //1.
        SaveKeyValues.putStringValues("plan_start_date",start_dates);
        SaveKeyValues.putIntValues("plan_start_year", start_year);
        SaveKeyValues.putIntValues("plan_start_month" , start_month);
        SaveKeyValues.putIntValues("plan_start_day" , start_date);
        //2.
        SaveKeyValues.putStringValues("plan_stop_date",stop_dates);
        SaveKeyValues.putIntValues("plan_stop_year", stop_year);
        SaveKeyValues.putIntValues("plan_stop_month" , stop_month);
        SaveKeyValues.putIntValues("plan_stop_day", stop_date);
        //3.
        SaveKeyValues.putFloatValues("plan_min_normal_weight_values",(float)((double)min_normal_weight));
        SaveKeyValues.putFloatValues("plan_max_normal_weight_values",(float)((double)max_normal_weight));
        //4.
        SaveKeyValues.putIntValues("plan_want_weight_values",plan_want_weight);
    }
    /**
     * hide back
     * @param keyCode
     * @param event
     * @return
     */
    @Override
    public boolean onKeyDown(int keyCode, KeyEvent event) {
        if(keyCode == KeyEvent.KEYCODE_BACK){
            return false;
        }
        return false;
    }
}