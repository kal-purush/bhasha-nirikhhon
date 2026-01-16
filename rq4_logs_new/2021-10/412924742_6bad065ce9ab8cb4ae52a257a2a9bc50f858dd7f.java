package br.com.brenohff.flutter_background;

import android.content.Intent;
import android.os.Bundle;

import androidx.annotation.Nullable;

import org.json.JSONException;
import org.json.JSONObject;

import io.flutter.FlutterInjector;
import io.flutter.embedding.android.FlutterActivity;
import io.flutter.embedding.engine.FlutterEngine;
import io.flutter.embedding.engine.dart.DartExecutor;
import io.flutter.plugin.common.JSONMethodCodec;
import io.flutter.plugin.common.MethodChannel;
import io.flutter.view.FlutterCallbackInformation;

public class MainActivity extends FlutterActivity {

    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        System.out.println("MainActivity.onCreate");

        new MethodChannel(getFlutterEngine().getDartExecutor().getBinaryMessenger(), "br.com.brenohff.background", JSONMethodCodec.INSTANCE).setMethodCallHandler((call, result) -> {
            JSONObject arg = (JSONObject) call.arguments;

            if (call.method.equals("startService")) {
                long callbackHandle;

                try {
                    callbackHandle = arg.getLong("handle");
                } catch (JSONException e) {
                    System.out.println("Failed read arguments");
                    result.error("100", "Failed read arguments", null);
                    return;
                }

                FlutterCallbackInformation callback = FlutterCallbackInformation.lookupCallbackInformation(callbackHandle);
                if (callback == null) {
                    System.out.println("Failed creating callback");
                    return;
                }

                DartExecutor.DartCallback dartCallback = new DartExecutor.DartCallback(getAssets(), FlutterInjector.instance().flutterLoader().findAppBundlePath(), callback);
                FlutterEngine backgroundEngine = new FlutterEngine(this);
                backgroundEngine.getDartExecutor().executeDartCallback(dartCallback);

                startService(new Intent(this, MyService.class));
                result.success(true);
                return;
            }

            if (call.method.equals("stopService")) {
                System.out.println("MyService.onCreate.MethodChannel.stopService");
                stopService(new Intent(this, MyService.class));
                result.success(true);
                return;
            }

            if (call.method.equals("statusService")) {
                result.success(true);
            }
        });
    }
}