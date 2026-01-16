package com.sarality.action;

import android.app.Activity;
import android.content.Intent;

/**
 * Action to refresh the existing Activity with the same intent.
 *
 * @author abhideep@ (Abhideep Singh)
 */
public class RefreshActivityAction implements ViewAction {

  private final Activity activity;

  public RefreshActivityAction(Activity activity) {
    this.activity = activity;
  }

  @Override
  public boolean perform(ActionContext actionContext) {
    Intent intent = activity.getIntent();
    activity.finish();
    intent.addFlags(Intent.FLAG_ACTIVITY_NO_ANIMATION);
    activity.startActivity(intent);
    return true;
  }
}