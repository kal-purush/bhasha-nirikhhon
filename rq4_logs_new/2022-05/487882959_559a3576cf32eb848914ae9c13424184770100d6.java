package com.umeng.umeng_sms_sdk;

import android.content.Context;
import android.os.Handler;
import android.os.Looper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.flutter.plugin.common.MethodCall;
import io.flutter.plugin.common.MethodChannel.Result;

import com.umeng.sms.listener.UMSMSCheckListener;
import com.umeng.sms.listener.UMSMSCodeListener;
import com.umeng.sms.UMSMS;

class UmengFlutterPluginForSms {

    static boolean verifyMethodCall(Context context, MethodCall call, Result result) {
        boolean resultCode = true;
        List<String> arguments = call.arguments();

        if ("getVersion".equals(call.method)) {
            _getVersion(result);
        } else if ("setSMSSDKInfo".equals(call.method)) {
            _setSMSSDKInfo(context, arguments);
        } else if ("getVerificationCode".equals(call.method)) {
            _getVerificationCode(arguments, result);
        } else if ("commitVerificationCode".equals(call.method)) {
            _commitVerificationCode(arguments, result);
        } else {
            resultCode = false;
        }

        return resultCode;
    }

    private static void _getVersion(Result result) {
        _executeOnMain(result, UMSMS.getVersion());
    }

    private static void _setSMSSDKInfo(Context context, List<String> arguments) {
        String info = arguments.get(0);
        UMSMS.setSMSSDKInfo(context, info);
    }

    private static void _getVerificationCode(List<String> arguments, final Result result) {
        String phoneNumber = arguments.get(0);
        String templateID = arguments.get(1);
        UMSMS.getVerificationCode(phoneNumber, templateID, new UMSMSCodeListener() {
            @Override
            public void getCodeSuccess(final String ret) {
                try {
                    Map<String, Object> map = new HashMap<String, Object>() {{
                        put("ret", ret);
                    }};
                    _executeOnMain(result, map);
                } catch (Throwable ignore) {
                    // Ignore exception
                }
            }

            @Override
            public void getCodeFailed(final int errCode, final String errMsg) {
                try {
                    Map<String, Object> map = new HashMap<String, Object>() {{
                        put("errCode", errCode);
                        put("errMsg", errMsg);
                    }};
                    _executeOnMain(result, map);
                } catch (Throwable ignore) {
                    // Ignore exception
                }
            }
        });
    }

    private static void _commitVerificationCode(List<String> arguments, final Result result) {
        String phoneNumber = arguments.get(0);
        String vCode = arguments.get(1);
        UMSMS.commitVerificationCode(phoneNumber, vCode, new UMSMSCheckListener() {
            @Override
            public void checkCodeSuccess(final String ret) {
                try {
                    Map<String, Object> map = new HashMap<String, Object>() {{
                        put("ret", ret);
                    }};
                    _executeOnMain(result, map);
                } catch (Throwable ignore) {
                    // Ignore exception
                }
            }

            @Override
            public void checkCodeFailed(final int errCode, final String errMsg) {
                try {
                    Map<String, Object> map = new HashMap<String, Object>() {{
                        put("errCode", errCode);
                        put("errMsg", errMsg);
                    }};
                    _executeOnMain(result, map);
                } catch (Throwable ignore) {
                    // Ignore exception
                }
            }
        });
    }

    private static final Handler _mHandler = new Handler(Looper.getMainLooper());

    private static void _executeOnMain(final Result result, final Object param) {
        if (Looper.myLooper() == Looper.getMainLooper()) {
            try {
                result.success(param);
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
            return;
        }
        _mHandler.post(new Runnable() {
            @Override
            public void run() {
                try {
                    result.success(param);
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
            }
        });
    }

}