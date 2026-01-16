package cz.slama.android.gtd.ui;

import android.content.Intent;
import android.os.Bundle;
public class FacebookActivity extends BootstrapActivity {

  ...

  @Override
  protected void onCreate(final Bundle savedInstanceState) {
    ...
    //facebookLoginButton is instance of com.facebook.login.widget.LoginButton
    facebookLoginButton
      .setPublishPermissions(Arrays.asList("publish_actions"));
    LoginManager.getInstance().registerCallback(callbackManager,
        new FacebookCallback<LoginResult>() {
          @Override
          public void onSuccess(LoginResult loginResult) {
            successLogin(loginResult);
          }

          @Override
          public void onCancel() {
            cancelLogin();
          }

          @Override
          public void onError(FacebookException exception) {
            errorLogin(exception);
          }
        });
  }

  ...

  private void successLogin(LoginResult loginResult) {
    ...
    if (loginResult.getRecentlyDeniedPermissions().size() > 0) {
      ...
      return;
    }
    ...
        
    accessToken = AccessToken.getCurrentAccessToken();
 
    ...
    
    ShrPrefUtils.saveFacebookToken(accessToken.getToken());
      
    ...
  }
}