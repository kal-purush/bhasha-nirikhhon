package wwtao.demo.demo_activityrouter;

import com.chenenyu.router.Router;

import android.app.Application;

/**
 * Created by wangweitao04 on 17/2/14.
 */

public class App extends Application {
    @Override
    public void onCreate() {
        super.onCreate();
        Router.initialize(this);
    }
}