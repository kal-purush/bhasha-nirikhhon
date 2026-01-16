package csr.capestart.com.firebase;

import android.content.Context;

import com.rx2androidnetworking.Rx2AndroidNetworking;

import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

import csr.capestart.com.data.ApiEndpoints;
import csr.capestart.com.extras.AppLog;
import csr.capestart.com.extras.SessionStore;
import csr.capestart.com.utils.StringUtils;
import io.reactivex.Observer;
import io.reactivex.android.schedulers.AndroidSchedulers;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;

public final class FirebaseUtils {

    private static final String TAG = "FirebaseUtils";

    private FirebaseUtils() {

    }

    public static void registerToken(Context context) {
        String token = SessionStore.getToken(context);
        if(StringUtils.isNotEmpty(token) && StringUtils.isNotEmpty(SessionStore.user.getEmail())) {
            Map<String, String> body = new HashMap<>();
            body.put("email", SessionStore.user.getEmail());
            body.put("device_token", token);
            Rx2AndroidNetworking
                    .post(ApiEndpoints.POST_REGISTER_DEVICE_TOKEN_API)
                    .addHeaders("Content-Type","application/json")
                    .addApplicationJsonBody(body)
                    .build()
                    .getJSONObjectObservable()
                    .subscribeOn(Schedulers.io())
                    .observeOn(AndroidSchedulers.mainThread())
                    .subscribe(new Observer<JSONObject>() {
                        @Override
                        public void onSubscribe(Disposable d) {

                        }

                        @Override
                        public void onNext(JSONObject user) {
                            AppLog.log(TAG, user.toString());
                        }

                        @Override
                        public void onError(Throwable e) {
                            AppLog.log(TAG, e.getMessage());
                        }

                        @Override
                        public void onComplete() {

                        }
                    });
        }
    }
}