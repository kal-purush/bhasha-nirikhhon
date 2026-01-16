package com.immortals.tw;

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.os.Message;
import android.util.Log;
import android.view.View;
import android.view.Window;
import android.view.WindowManager;
import android.widget.EditText;
import android.widget.Spinner;
import android.widget.TextView;

import com.global.sdk.NCGSDK;
import com.global.sdk.base.NCGActionCode;
import com.global.sdk.listenner.GlobalCallback;
import com.global.sdk.manager.NCGCallback;
import com.global.sdk.model.TranslationBean;
import com.global.sdk.util.ToastUtil;
import com.gm88.gmutils.SDKLog;
import com.gm88.gmutils.ToastHelper;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

public class MainActivity extends Activity implements View.OnClickListener {

    private TextView mTvEnterQufu, mTvEnterRole, mTvEnterGame, mTvDoPay, mTvShare, mLogin, mTvToCustomer,
            mTvCreateRole, mTvOverBegin, mTvLevel, mFBReLoading, mTvShowBind, mTvSendEvent;
    private TextView mTvTranslation, mTvPayList, mTvSwitchAccount,mTvVideoPlayLand,mTvVideoPlayPortrait,mTvGetDeviceInfo;
    private TextView mTvUsercenter;
    private EditText mEtOrderName;
    private EditText mEtOrderPrice;
    private EditText mEtProductId;
    private EditText mEtTranslation;
    private EditText mEtVideoUrl;
    private Spinner mEtEvent;
    private static final String TAG = "MainActivity";

    @Override
    protected void onCreate(Bundle savedInstanceState) {

        requestWindowFeature(Window.FEATURE_NO_TITLE);
        getWindow().setFlags(WindowManager.LayoutParams.FLAG_FULLSCREEN, WindowManager.LayoutParams.FLAG_FULLSCREEN);
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        init();
        initView();

    }


    private void init() {
        NCGSDK.setCallBack(new NCGCallback() {
            @Override
            public void onCallBack(final Message msg) {
                switch (msg.what) {
                    case NCGActionCode.ACTION_INIT_SUCC://初始化成功
                        ToastHelper.toast(MainActivity.this, "初始化完成");
                        NCGSDK.doLogin();
                        break;
                    case NCGActionCode.ACTION_INIT_FAILED://初始化失败
                        finish();
                        break;
                    case NCGActionCode.ACTION_LOGIN_SUCC://登录成功
                        String spot = "{\"spotType\":4,\"extra\":{\"roleName\":\"IleanaJudd\",\"vipLevel\":3,\"serverName\":1服,\"zone\":001,\"roleServer\":55,\"roleLevel\":1,\"roleId\":1234567,\"globalRoleId\":ncg_70_10691603" + "}}";
                        NCGSDK.doSpot(spot);
                        JSONObject result = (JSONObject) msg.obj;
                        Log.e(TAG, "登录成功" + result.toString());
                        ToastHelper.toast(MainActivity.this, result.toString());
                        break;
                    case NCGActionCode.ACTION_LOGIN_CANCEL://退出登录
//                        toast("退出登录");
                        break;
                    case NCGActionCode.ACTION_LOGIN_FAILED://登录失败
                        ToastHelper.toast(MainActivity.this, "登录失败" + msg.obj);
                        break;
                    case NCGActionCode.ACTION_LOGOUT_SUCC://登出成功
//                        toast("登出成功");
                        break;
                    case NCGActionCode.ACTION_GAME_EXIT://退出游戏
//                        toast("退出游戏");
                        break;
                    case NCGActionCode.ACTION_LOGOUT_FAILED://登出失败，一般不会出现，出现代表有问题
                        break;
                    case NCGActionCode.ACTION_PAY_SUCC://支付成功
//                        toast("支付成功");
                        break;
                    case NCGActionCode.ACTION_PAY_CANCEL://用户退出支付
//                        toast("用户退出支付");
                        break;
                    case NCGActionCode.ACTION_TRANSLATION_SUCCESS://翻译成功
                        TranslationBean translationBean = (TranslationBean) msg.obj;
                        ToastHelper.toast(MainActivity.this, "翻译成功\n" + translationBean.getTarget_text() + "\n" + translationBean.getExtra());
                        break;
                    case NCGActionCode.ACTION_TRANSLATION_FAILED://翻译失败
                        ToastHelper.toast(MainActivity.this, "翻译失败\n" + String.valueOf(msg.obj));
                    default:
                        break;
                }
            }
        });
        NCGSDK.initMainActivity(MainActivity.this);
    }

    private void initView() {
        mLogin = findViewById(R.id.game_login);
        mTvEnterQufu = findViewById(R.id.game_qufu);
        mTvEnterRole = findViewById(R.id.game_createrole);
        mTvEnterGame = findViewById(R.id.game_in);
        mTvDoPay = findViewById(R.id.game_pay);
        mTvShare = findViewById(R.id.game_share);
        mTvCreateRole = findViewById(R.id.game_create);
        mTvOverBegin = findViewById(R.id.game_tutorial);
        mTvLevel = findViewById(R.id.game_level);
        mTvShowBind = findViewById(R.id.game_showbind);
        mTvPayList = findViewById(R.id.game_get_pay_list);
        mTvSendEvent = findViewById(R.id.game_send_affb);
        mFBReLoading = findViewById(R.id.game_load_fb_initad);
        mTvTranslation = findViewById(R.id.game_translation);
        mTvSwitchAccount = findViewById(R.id.game_switch_account);
        mEtTranslation = findViewById(R.id.game_translation_targettext);
        mEtEvent = findViewById(R.id.even_sp);
        mTvToCustomer = findViewById(R.id.game_create_deeplink);
        mEtVideoUrl = findViewById(R.id.game_videoUrl);
        mTvVideoPlayLand = findViewById(R.id.game_playVideo_land);
        mTvVideoPlayPortrait = findViewById(R.id.game_playVideo_portrait);
        mTvGetDeviceInfo = findViewById(R.id.game_getDeviceInfo);
        mTvUsercenter = findViewById(R.id.game_usercenter);
        mEtOrderName = findViewById(R.id.game_order);
        mEtOrderPrice = findViewById(R.id.game_price);
        mEtProductId = findViewById(R.id.game_productid);
        mTvTranslation.setOnClickListener(this);
        mLogin.setOnClickListener(this);
        mTvPayList.setOnClickListener(this);
        mTvShowBind.setOnClickListener(this);
        mTvSendEvent.setOnClickListener(this);
        mTvSwitchAccount.setOnClickListener(this);
        mTvToCustomer.setOnClickListener(this);
        mTvEnterQufu.setOnClickListener(this);
        mTvEnterRole.setOnClickListener(this);
        mTvEnterGame.setOnClickListener(this);
        mTvDoPay.setOnClickListener(this);
        mTvShare.setOnClickListener(this);
        mTvCreateRole.setOnClickListener(this);
        mTvOverBegin.setOnClickListener(this);
        mTvLevel.setOnClickListener(this);
        mFBReLoading.setOnClickListener(this);
        mTvVideoPlayLand.setOnClickListener(this);
        mTvVideoPlayPortrait.setOnClickListener(this);
        mTvGetDeviceInfo.setOnClickListener(this);
        mTvUsercenter.setOnClickListener(this);

    }

    @Override
    public void onClick(View v) {
        switch (v.getId()) {
            case R.id.game_login:
                SDKLog.e(TAG, "点击登录");
                NCGSDK.doLogin();
                break;
            case R.id.game_pay:
                SDKLog.e(TAG, "点击支付");
                Map<String, String> payinfo = new HashMap<>();
                if (mEtOrderName.getText().toString().trim().isEmpty()) {
                    payinfo.put("productName", "1001-60元寶");
                } else {
                    payinfo.put("productName", mEtOrderName.getText().toString().trim());
                }
                if (mEtOrderPrice.getText().toString().trim().isEmpty()) {
                    payinfo.put("productPrice", "0.99");
                } else {
                    payinfo.put("productPrice", mEtOrderPrice.getText().toString().trim());
                }
                if (mEtProductId.getText().toString().trim().isEmpty()) {
                    payinfo.put("productId", "1001");
                } else {
                    payinfo.put("productId", mEtProductId.getText().toString().trim());
                }
                payinfo.put("roleId", "1");
                payinfo.put("roleName", "1");
                payinfo.put("serverId", "1");
                payinfo.put("serverName", "1");
                payinfo.put("notifyUrl", "");
                payinfo.put("extra", System.currentTimeMillis() / 1000 + "");
                NCGSDK.doPay(payinfo);
                break;
            case R.id.game_share:

                JSONObject jsonObject1 = new JSONObject();
                try {
                    jsonObject1.put("shareID", "1");
                    jsonObject1.put("shareName", "分享");
                    jsonObject1.put("uName", "11");
                    jsonObject1.put("server", "2");
                    jsonObject1.put("code", "3");
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                NCGSDK.doShare(jsonObject1.toString());
                break;
            case R.id.game_showbind:
                NCGSDK.showBind();
                break;
            case R.id.game_get_pay_list:
                NCGSDK.getPurchaseList(new GlobalCallback() {
                    @Override
                    public void onSuccess(String o) {
                        ToastHelper.toast(MainActivity.this, o);
                        SDKLog.d(TAG, "doPurchaseListDone=" + o);
                    }
                    @Override
                    public void onFailed(String msg) {
                        SDKLog.d(TAG, "doPurchaseListDone=" + msg);
                    }
                });
                break;
            case R.id.game_translation:
                NCGSDK.translation2Text("111", mEtTranslation.getText().toString().isEmpty() ? "hello" : mEtTranslation.getText().toString());
                break;
            case R.id.game_switch_account:
                NCGSDK.showLogin();
                break;
            case R.id.game_send_affb:
                NCGSDK.doEventInfo(mEtEvent.getSelectedItem().toString());
                break;
            case R.id.game_create_deeplink:
                NCGSDK.showServiceCenter();
                break;
            case R.id.game_playVideo_land:
                String videoUrl = mEtVideoUrl.getText().toString().trim();
                NCGSDK.playVideo(videoUrl, "",0);
                break;
            case R.id.game_playVideo_portrait:
                String videoUrl2 = mEtVideoUrl.getText().toString().trim();
                NCGSDK.playVideo(videoUrl2, "",1);
                break;
            case R.id.game_getDeviceInfo:
                String deviceInfo = NCGSDK.getDeviceInfo();
                ToastUtil.toast(this, deviceInfo);
                break;
            case R.id.game_usercenter:
                NCGSDK.showUserCenter();
                break;
            default:
                break;
        }
    }

    @Override
    public void onBackPressed() {
        NCGSDK.onBackPressed();
    }

    @Override
    protected void onResume() {
        super.onResume();
        NCGSDK.onResume(this);
    }

    @Override
    protected void onPause() {
        super.onPause();
        NCGSDK.onPause();
    }

    @Override
    protected void onDestroy() {
        super.onDestroy();
        NCGSDK.onDestroy();
    }


    @Override
    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        super.onActivityResult(requestCode, resultCode, data);
        NCGSDK.onActivityResult(requestCode, resultCode, data);
    }

}