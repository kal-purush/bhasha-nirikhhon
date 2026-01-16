package com.yundian.celebrity.ui.main.activity;

import android.Manifest;
import android.content.DialogInterface;
import android.content.pm.PackageManager;
import android.os.Build;
import android.os.Environment;
import android.support.v4.content.PermissionChecker;
import android.support.v7.app.AlertDialog;
import android.text.TextUtils;
import android.util.Log;
import android.view.View;
import android.widget.ImageView;
import android.widget.RelativeLayout;
import android.widget.TextView;
import android.widget.Toast;

import com.liulishuo.engzo.lingorecorder.LingoRecorder;
import com.liulishuo.engzo.lingorecorder.processor.TimerProcessor;
import com.qiniu.android.common.Zone;
import com.qiniu.android.http.ResponseInfo;
import com.qiniu.android.storage.Configuration;
import com.qiniu.android.storage.UpCompletionHandler;
import com.qiniu.android.storage.UploadManager;
import com.yundian.celebrity.R;
import com.yundian.celebrity.app.AppConfig;
import com.yundian.celebrity.base.BaseActivity;
import com.yundian.celebrity.bean.EventBusMessage;
import com.yundian.celebrity.bean.RequestResultBean;
import com.yundian.celebrity.bean.UptokenBean;
import com.yundian.celebrity.listener.OnAPIListener;
import com.yundian.celebrity.networkapi.NetworkAPIFactoryImpl;
import com.yundian.celebrity.utils.FormatUtil;
import com.yundian.celebrity.utils.LogUtils;
import com.yundian.celebrity.utils.SharePrefUtil;
import com.yundian.celebrity.utils.ToastUtils;
import com.yundian.celebrity.widget.NormalTitleBar;
import com.yundian.celebrity.widget.photobutton.CaptureButton;
import com.yundian.celebrity.widget.photobutton.lisenter.CaptureLisenter;

import org.greenrobot.eventbus.EventBus;
import org.json.JSONObject;

import java.io.File;

import butterknife.Bind;

/**
 * 定制语音
 * Created by sll on 2017/7/13.
 */

public class RecordAudioActivity extends BaseActivity {

    @Bind(R.id.nt_title)
    NormalTitleBar ntTitle;
    @Bind(R.id.capture_button_view)
    CaptureButton  capture_button;
    public static final String TIMER = "timer";

    private String pathUrl = "";
    private UploadManager uploadManager;

    private int layout_width;
    private int layout_height;
    private int button_size;
    private RelativeLayout audioLayout;
    private TextView info;

    private ImageView micIcon;
    private LingoRecorder lingoRecorder;

    @Override
    public int getLayoutId() {
        return R.layout.activity_record_audio;
    }

    @Override
    public void initPresenter() {

    }
    Zone zone=Zone.zone0;
    boolean isCancel;
    @Override
    public void initView() {
        lingoRecorder = new LingoRecorder();
        //get width
//        WindowManager manager = (WindowManager) this.getSystemService(Context.WINDOW_SERVICE);
//        DisplayMetrics outMetrics = new DisplayMetrics();
//        manager.getDefaultDisplay().getMetrics(outMetrics);
//
//        if (this.getResources().getConfiguration().orientation ==android.content.res.Configuration.ORIENTATION_PORTRAIT) {
//            layout_width = outMetrics.widthPixels;
//        } else {
//            layout_width = outMetrics.widthPixels / 2;
//        }
//        button_size = (int) (layout_width / 4.5f);
//        layout_height = button_size + (button_size / 5) * 2 + 100;
//
//        button_size = (int) (layout_width / 4.5f);
//        CaptureButton btn_capture = new CaptureButton(this, button_size);
//        FrameLayout.LayoutParams btn_capture_param = new FrameLayout.LayoutParams(FrameLayout.LayoutParams.MATCH_PARENT, FrameLayout.LayoutParams.MATCH_PARENT);
////        btn_capture_param.addRule(CENTER_IN_PARENT, TRUE);
//        btn_capture_param.gravity = Gravity.BOTTOM;
////        btn_capture_param.setMargins(0, 152, 0, 0);
//        btn_capture.setLayoutParams(btn_capture_param);
//        btn_capture.setDuration(10 * 1000);
//        ((ViewGroup)rootView).addView(btn_capture);

        ntTitle.setTitleText("定制语音");
        ntTitle.setRightTitleVisibility(true);
        ntTitle.setRightTitle("确认提交");
        ntTitle.setRightTitleColor(R.color.color_8C0808);


        audioLayout = (RelativeLayout)findViewById(R.id.audio_layout);
        info = (TextView)findViewById(R.id.tv_info);
        micIcon = (ImageView)findViewById(R.id.mic_icon);
        capture_button.setCaptureLisenter(new CaptureLisenter() {
            @Override
            public void takePictures() {

            }

            @Override
            public void recordShort(long time) {

            }

            @Override
            public void recordStart() {
                startAnim(true);
                startRecord();
//                ret = true;
            }

            @Override
            public void recordEnd(long time) {
                if (isCancel) {
                    isCancel = false;
                    stopRecordUnSave(false);
                    Toast.makeText(RecordAudioActivity.this, "取消保存", Toast.LENGTH_SHORT).show();
                    //如果是录制结束抬手
                } else {

                    TimerProcessor timerProcessor = (TimerProcessor) lingoRecorder.getWithKey(TIMER);
                    if(timerProcessor !=null){
                        long flowTime = timerProcessor.getFlowTime();
                        if(flowTime<2000){
                            stopRecordUnSave(true);
                        }else{
                            stopRecordWithSave();
                        }
                    }

                }
            }

            @Override
            public void recordZoom(float zoom) {
                if (zoom > 10) {
                    moveAnim();
                    isCancel = true;
                } else {
                    isCancel = false;
                    startAnim(false);
                }
            }

            @Override
            public void recordError() {

            }
        });
//        ntTitle.setLeftTitle("取消");
//        ntTitle.setTvLeftVisiable(true);
//        ntTitle.setLeftImagSrc(0);  //去掉返回图标

        initListener();

        setZone();


    }

    public void stopRecordWithSave() {
        if (lingoRecorder.isAvailable()) {
            if (lingoRecorder.isRecording()) {
                lingoRecorder.stop();
//                Toast.makeText(DemoActivity.this, "文件以保存至：" + path, Toast.LENGTH_SHORT).show();
            }
        }
    }

    public static final String SAVE_PATH = Environment.getExternalStoragePublicDirectory(Environment.DIRECTORY_MUSIC).getPath();
    public static final String SAVE_NAME_WAV = File.separator+"test.wav";
    public void stopRecordUnSave(boolean isTimeShort) {
        if (lingoRecorder.isAvailable()) {
            if (lingoRecorder.isRecording()) {
                lingoRecorder.stop();
//                File targetFile = new File("/sdcard/test.wav");

                File targetFile = new File(SAVE_PATH+SAVE_NAME_WAV);
                if (targetFile.exists()) {
                    //不保存直接删掉
                    targetFile.delete();
                    if(isTimeShort){
                        Toast.makeText(RecordAudioActivity.this, "时间太短,已删", Toast.LENGTH_SHORT).show();
                    }else{
                        Toast.makeText(RecordAudioActivity.this, "文件已删除", Toast.LENGTH_SHORT).show();
                    }
                }

            }
        }
    }
    private static final int REQUEST_CODE_PERMISSION = 10010;
    //检查权限
    private boolean checkRecordPermission() {
        if (PermissionChecker.checkSelfPermission(this, Manifest.permission.WRITE_EXTERNAL_STORAGE) != PackageManager.PERMISSION_GRANTED
                || PermissionChecker.checkSelfPermission(this, Manifest.permission.RECORD_AUDIO) != PackageManager.PERMISSION_GRANTED) {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
                if (shouldShowRequestPermissionRationale(Manifest.permission.WRITE_EXTERNAL_STORAGE) ||
                        shouldShowRequestPermissionRationale(Manifest.permission.RECORD_AUDIO)) {
                    new AlertDialog.Builder(this)
                            .setTitle("检查权限")
                            .setMessage("需要权限")
                            .setCancelable(false)
                            .setPositiveButton("ok", new DialogInterface.OnClickListener() {
                                @Override
                                public void onClick(DialogInterface dialog, int which) {
                                    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
                                        requestPermissions(new String[]{Manifest.permission.WRITE_EXTERNAL_STORAGE, Manifest.permission.RECORD_AUDIO}, REQUEST_CODE_PERMISSION);
                                    }
                                }
                            }).show();
                } else {
                    requestPermissions(new String[]{Manifest.permission.WRITE_EXTERNAL_STORAGE, Manifest.permission.RECORD_AUDIO}, REQUEST_CODE_PERMISSION);
                }
            }
            return false;
        }
        return true;
    }
    public void startRecord() {
        if (!checkRecordPermission()) {
            return;
        }
        // isAvailable 为 false 为录音正在处理时，需要保护避免在这个时候操作录音器
//        if (lingoRecorder.isAvailable()) {
//            if (lingoRecorder.isRecording()) {
//                lingoRecorder.stop();
//            } else {
//                // need get permission
//                lingoRecorder.start("/sdcard/test2.wav");
//                resultView.setText("");
//                recordBtn.setText("stop");
//            }
//        }

        if (lingoRecorder.isAvailable()) {
            if (!lingoRecorder.isRecording()) {
                // need get permission

                //不执行这个,这种方式转码的processor可能会block掉record的线程,导致录音不全
//                lingoRecorder.start("/sdcard/test2.wav");
                //执行这个,这种方式是优化过得,分别在两条线程里玩
                lingoRecorder.start();
//                resultView.setText("");
//                recordBtn.setText("stop");

//            }
            }
        }
    }

    private void startAnim(boolean isStart){
        audioLayout.setVisibility(View.VISIBLE);
        info.setText("上滑取消");
//        recordBtn.setBackgroundDrawable(getResources().getDrawable(R.drawable.mic_pressed_bg));
        micIcon.setBackgroundDrawable(null);
//        micIcon.setBackgroundDrawable(getResources().getDrawable(R.drawable.ic_mic_white_24dp));
        if (isStart){
//            chronometer.setBase(SystemClock.elapsedRealtime());
//            chronometer.setFormat("%S");
//            chronometer.start();
        }
    }

    private void moveAnim(){
        info.setText("松手取消");
        micIcon.setBackgroundDrawable(null);
//        micIcon.setBackgroundDrawable(getResources().getDrawable(R.drawable.ic_undo_black_24dp));
    }

    private void setZone() {
        if ("华东".equals(AppConfig.AREA)){
            zone= Zone.zone0;
        }else if("华南".equals(AppConfig.AREA)){
            zone=Zone.zone2;
        }else if("华北".equals(AppConfig.AREA)){
            zone=Zone.zone1;
        }

        Configuration config = new Configuration.Builder()
                .zone(Zone.zone2) // 设置区域，指定不同区域的上传域名、备用域名、备用IP。
                .build();

        uploadManager = new UploadManager(config);
//        uploadManager = new UploadManager();
    }

    // TODO: 2017/8/29
    private void initListener() {
//        ntTitle.setOnRightTextListener(new View.OnClickListener() {
//            @Override
//            public void onClick(View v) {
//                if (TextUtils.isEmpty(feedbackContent.getText().toString().trim()) &&
//                        TextUtils.isEmpty(pathUrl)) {
//                    ToastUtils.showShort("发布内容不能为空");
//                } else {
//                    publishState();//发布状态
//                }
//            }
//        });

    }


//    private UploadManager uploadManager = new UploadManager();

    private void publishState() {


        if(!TextUtils.isEmpty(pathUrl)){
            startProgressDialog();
            NetworkAPIFactoryImpl.getUserAPI().getQiNiuToken(new OnAPIListener<UptokenBean>() {
                @Override
                public void onError(Throwable ex) {

                }

                @Override
                public void onSuccess(final UptokenBean uptokenBean) {
                    LogUtils.loge("请求到的http数据:" + uptokenBean);
//                    final QiNiuImageToken tokenEntity = JSON.parseObject(uptokenBean, QiNiuImageToken.class);
                    runOnUiThread(new Runnable() {
                        @Override
                        public void run() {
                            LogUtils.loge("生成的图片名称是:" + FormatUtil.createImageName());
                            String extName = getExtName(pathUrl, '.');

                            uploadManager.put(pathUrl, FormatUtil.createImageName()+extName, uptokenBean.getUptoken(),
                                    new UpCompletionHandler() {
                                        @Override
                                        public void complete(String key, ResponseInfo info, JSONObject response) {
                                            //res包含hash、key等信息，具体字段取决于上传策略的设置
                                            if (info.isOK()) {
                                                Log.i("qiniu", "Upload Success");

                                                //拿到上传的图片地址,请求自己的服务器
//                                                String imageUrl = Constant.QI_NIU_BASE_URL + key;
                                                //星享要拼接区域
                                                String imageUrl =AppConfig.HUANAN_QI_NIU_PIC_ADRESS+ key;
                                                LogUtils.loge("获取的图片地址:" + imageUrl);
                                                doSendContent(imageUrl);
                                            } else {
                                                Log.i("qiniu", "Upload Fail");
                                                stopProgressDialog();
                                                //如果失败，这里可以把info信息上报自己的服务器，便于后面分析上传错误原因
                                            }
                                            Log.i("qiniu", key + ",\r\n " + info + ",\r\n " + response);

                                        }
                                    }, null);
                        }
                    });
                }
            });

        }else{
            ToastUtils.showShort("请选择一张图片");
        }
    }

    private String getExtName(String s, char split) {
        int i = s.lastIndexOf(split);
        int leg = s.length();
        String s1 = i > 0 ? (i + 1) == leg ? " " : s.substring(i, s.length()) : " ";
        return s1;
    }
    //上传到自己的服务器里
    private void doSendContent(String imageUrl) {

        String starCode = SharePrefUtil.getInstance().getStarcode();
        // TODO: 2017/8/29
        NetworkAPIFactoryImpl.getDealAPI().publishState("", imageUrl, starCode, new OnAPIListener<RequestResultBean>() {
            @Override
            public void onError(Throwable ex) {
                LogUtils.loge("发布失败");
                ToastUtils.showShort("发布失败");
                stopProgressDialog();

            }

            @Override
            public void onSuccess(RequestResultBean requestResultBean) {

                if (requestResultBean.getCircle_id() != -1) {
                    EventBus.getDefault().post(new EventBusMessage(-65));
                    ToastUtils.showShort("发布成功");

                    finish();
                    stopProgressDialog();

                }
            }
        });
    }





}