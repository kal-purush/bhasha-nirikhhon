package com.tvnsoftware.drcare.activity.Alarm;

import android.content.Intent;
import android.os.Build;
import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.Toolbar;
import android.util.Log;
import android.util.Pair;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.CheckBox;
import android.widget.EditText;
import android.widget.TextView;

import com.afollestad.materialdialogs.MaterialDialog;
import com.tvnsoftware.drcare.R;
import com.tvnsoftware.drcare.model.Remind;
import com.tvnsoftware.drcare.model.Repeat;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import butterknife.BindView;
import butterknife.ButterKnife;
import butterknife.OnClick;

import static com.tvnsoftware.drcare.activity.AlarmActivity.EXTRA_ALARM_UPDATE;
import static com.tvnsoftware.drcare.activity.AlarmActivity.EXTRA_IS_UPDATE;
import static com.tvnsoftware.drcare.activity.AlarmActivity.NEW_REMIND;


public class AlarmDetailsActivity extends AppCompatActivity {

    private static boolean IS_UPDATE_ALARM;
    private static Remind remind;
    private static List<Repeat> repeatList;

    @BindView(R.id.toolbar)
    Toolbar toolbar;
    @BindView(R.id.TimePicker)
    android.widget.TimePicker  tvTimePicker;
    @BindView(R.id.etNoteAlarm)
    EditText etNoteAlarm;
    @BindView(R.id.tvChosenRepeat)
    TextView tvChosenRepeat;
    @BindView(R.id.tvExpireAlarm)
    TextView tvExpireAlarm;
    @BindView(R.id.cbIsSnooze)
    CheckBox cbIsSnooze;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_alarm_details);
        ButterKnife.bind(this);

        setSupportActionBar(toolbar);
        setupToolbar();

        IS_UPDATE_ALARM = getIntent().getBooleanExtra(EXTRA_IS_UPDATE, false);
        remind = getIntent().getParcelableExtra(EXTRA_ALARM_UPDATE);

        prepareData_RepeatList();

        setUpViewNormal();

        if(IS_UPDATE_ALARM){
            setupViewToUpdate();
        }
    }

    private void prepareData_RepeatList(){
        // TODO: 31-Jul-17 : get LIST REPEAT NAME
        repeatList = new ArrayList<>();
        repeatList.add(new Repeat(1, "Never"));
        repeatList.add(new Repeat(2, "Every Day"));
    }

    private void setUpViewNormal() {
        Calendar now = Calendar.getInstance();
        tvTimePicker.setIs24HourView(true);
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            tvTimePicker.setMinute(Calendar.MINUTE);
            tvTimePicker.setHour(Calendar.HOUR_OF_DAY);
        }
        else {
            tvTimePicker.setCurrentHour(Calendar.HOUR);
            tvTimePicker.setCurrentMinute(Calendar.MINUTE);
        }
        //java.util.Calendar.HOUR_OF_DAY + ":" + java.util.Calendar.MINUTE);
        cbIsSnooze.setChecked(true);
        tvExpireAlarm.setText("dayCreate" + "presciption.getDays");
        tvChosenRepeat.setText(repeatList.get(1).getRepeatDay());
        etNoteAlarm.setText("Take Medicine");
    }

    private void setupViewToUpdate() {
        cbIsSnooze.setChecked(remind.getIsActivate());

        // TODO: 01-Aug-17 remind.getRemindList().getRemindID().getRepeatDay()
        tvChosenRepeat.setText(repeatList.get(1).getRepeatDay());

        etNoteAlarm.setText(remind.getRemindLabel());
        //tvTimePicker.setText(remind.getRemindTime());
        String strTime = remind.getRemindTime();
        int index = strTime.indexOf(':');
        String strHour = strTime.substring(0, index );
        String strMins = strTime.substring(index + 1, strTime.length());
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            tvTimePicker.setMinute(Integer.getInteger(strMins));
            tvTimePicker.setHour(Integer.getInteger(strHour));
        }
        else {
            Log.d("TimePicker: ", "test: " + strHour + ":" + strMins);
            tvTimePicker.setCurrentHour(Integer.valueOf(strHour));
            tvTimePicker.setCurrentMinute(Integer.valueOf(strMins));
        }
    }

    public static Pair<Integer, Integer> getHour_Mins(String strTime){
        int index = strTime.indexOf(':');
        String strHour = strTime.substring(0, index );
        String strMins = strTime.substring(index + 1, strTime.length());

        return new Pair<>(Integer.valueOf(strHour),
                Integer.valueOf(strMins));
    }

    private CharSequence[] adapterRepeat(){
        List<String> repeatDay = new ArrayList<>();
        for (Repeat item: repeatList) {
            repeatDay.add(item.getRepeatDay());
        }
        return repeatDay.toArray(new CharSequence[repeatList.size()]);
    }

    @OnClick(R.id.tvChosenRepeat)
    public void onClickChosenRepeat(){
        new MaterialDialog.Builder(this)
                .title("Repeat time")
                .items(adapterRepeat())
                .alwaysCallSingleChoiceCallback()
                .itemsCallbackSingleChoice(-1, new MaterialDialog.ListCallbackSingleChoice() {
                    @Override
                    public boolean onSelection(MaterialDialog dialog, View itemView, int which, CharSequence text) {
                        tvChosenRepeat.setText(text);
                        // TODO: 01-Aug-17 newIsRepeat = which
                        return true;
                    }
                })
                .positiveText("Save")
                .show();
    }

    /*set home button on toolbar */
    private void setupToolbar(){
        getSupportActionBar().setDisplayHomeAsUpEnabled(true);
        getSupportActionBar().setDisplayShowHomeEnabled(true);
    }

    @Override
    public boolean onSupportNavigateUp() {
        onBackPressed();
        return true;
    }

    /*set menu (button) on toolbar */
    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        getMenuInflater().inflate(R.menu.menu_alarm, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        switch (item.getItemId()){
            case R.id.item_done:
                onClickSaveAlarm();
        }
        return  true;
    }

    public void onClickSaveAlarm(){
        // TODO: 02-Aug-17 parse result to previous Activity
        Remind newRemind = new Remind();
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            newRemind.setRemindTime(tvTimePicker.getHour() + ":" + tvTimePicker.getMinute());
        }
        else {
            newRemind.setRemindTime(tvTimePicker.getCurrentHour() + ":"
                    + tvTimePicker.getCurrentMinute());
        }
        newRemind.setRemindLabel(etNoteAlarm.getText().toString());
        newRemind.setRepeatDay(tvChosenRepeat.getText().toString());
        newRemind.setIsActivate(true);
        newRemind.setIsSnooze(cbIsSnooze.isChecked());

        Intent dataResult = new Intent();
        dataResult.putExtra(NEW_REMIND, newRemind);
        setResult(RESULT_OK, dataResult);
        finish();
    }

    @Override
    public void onBackPressed() {
        finish();
    }
}