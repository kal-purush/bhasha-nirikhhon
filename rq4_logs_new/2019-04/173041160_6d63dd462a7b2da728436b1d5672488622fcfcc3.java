package np.com.naxa.factsnepal.common;

import android.app.Activity;
import android.content.Context;
import android.support.annotation.NonNull;
import android.util.Log;
import android.widget.Toast;

import com.facebook.AccessToken;
import com.facebook.FacebookRequestError;
import com.facebook.GraphRequest;
import com.facebook.GraphResponse;
import com.facebook.HttpMethod;
import com.facebook.login.LoginManager;
import com.google.android.gms.auth.api.signin.GoogleSignIn;
import com.google.android.gms.auth.api.signin.GoogleSignInClient;
import com.google.android.gms.auth.api.signin.GoogleSignInOptions;
import com.google.android.gms.tasks.OnCompleteListener;
import com.google.android.gms.tasks.Task;

import java.util.concurrent.Executor;

import np.com.naxa.factsnepal.utils.SharedPreferenceUtils;


public abstract class BaseLogout {

    private static final String TAG = "BaseLogout";

    Context context;
    SharedPreferenceUtils sharedPreferenceUtils;

    public BaseLogout(Context context) {
        this.context = context;
        sharedPreferenceUtils = new SharedPreferenceUtils(context);
        if (sharedPreferenceUtils.getIntValue(BaseLoginActivity.KEY_LOGGED_IN_TYPE, -1)== BaseLoginActivity.GMAIL_LOG_IN) {
            googleLogout();
        }else if(sharedPreferenceUtils.getIntValue(BaseLoginActivity.KEY_LOGGED_IN_TYPE, -1)== BaseLoginActivity.FACEBOOK_LOG_IN){
            facebookLogout();
        }
    }

    public abstract void onLogoutSuccess();

    public void googleLogout() {
        GoogleSignInOptions gso = new GoogleSignInOptions.Builder(GoogleSignInOptions.DEFAULT_SIGN_IN)
                .requestEmail()
                .build();
        GoogleSignInClient mGoogleSignInClient = GoogleSignIn.getClient(context, gso);
        mGoogleSignInClient.signOut()
                .addOnCompleteListener((Activity) context, new OnCompleteListener<Void>() {
                    @Override
                    public void onComplete(@NonNull Task<Void> task) {
                        // ...
                        if (task.isSuccessful()) {
                            Toast.makeText(context, "Gmail Logout Successfully", Toast.LENGTH_SHORT).show();
                            onLogoutSuccess();
                        }else {
                            Toast.makeText(context, "Logout error gmail", Toast.LENGTH_SHORT).show();

                        }
                    }
                });
    }


    public void facebookLogout() {
        onLogoutSuccess();

        GraphRequest delPermRequest = new GraphRequest(AccessToken.getCurrentAccessToken(), "/{user-id}/permissions/", null, HttpMethod.DELETE, new GraphRequest.Callback() {
            @Override
            public void onCompleted(GraphResponse graphResponse) {
                if (graphResponse != null) {
                    FacebookRequestError error = graphResponse.getError();
                    if (error != null) {
                        Log.e(TAG, error.toString());
                        LoginManager.getInstance().logOut();
                        Toast.makeText(context, "Facebook Logout Successfully", Toast.LENGTH_SHORT).show();
                        onLogoutSuccess();

                    } else {
                        Toast.makeText(context, "Logout error facebook", Toast.LENGTH_SHORT).show();
                    }
                }
            }
        });
        Log.d(TAG, "Executing revoke permissions with graph path" + delPermRequest.getGraphPath());
        delPermRequest.executeAsync();
    }

}