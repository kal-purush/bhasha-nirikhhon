package com.sarality.action;

import android.app.Activity;
import android.widget.Toast;

/**
 * Action to refresh the existing Activity with the same intent.
 *
 * @author abhideep@ (Abhideep Singh)
 */
public class DisplayToastAction implements ViewAction {

  private final Activity activity;
  private final int resId;
  private final boolean longDuration;

  public DisplayToastAction(Activity activity, int resId) {
    this(activity, resId, false);
  }

  private DisplayToastAction(Activity activity, int resId, boolean longDuration) {
    this.activity = activity;
    this.resId = resId;
    this.longDuration = longDuration;
  }

  @Override
  public boolean perform(ActionContext actionContext) {
    if (longDuration) {
      Toast.makeText(activity, resId, Toast.LENGTH_LONG).show();
    } else {
      Toast.makeText(activity, resId, Toast.LENGTH_SHORT).show();
    }
    return true;
  }
}