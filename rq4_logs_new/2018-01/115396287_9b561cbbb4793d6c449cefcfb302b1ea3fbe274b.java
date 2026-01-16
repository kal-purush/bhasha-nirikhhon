package com.simpletour.lib.apicontrol;

import android.annotation.TargetApi;
import android.app.Activity;
import android.app.ActivityManager;
import android.app.Dialog;
import android.app.Fragment;
import android.app.Service;
import android.content.Context;
import android.os.Build;
import android.view.View;

import java.lang.ref.WeakReference;
import java.util.List;

/**
 * 包名：com.simpletour.lib.apicontrol
 * 描述：ContextHolder
 * 创建者：yankebin
 * 日期：2018/1/14
 */
public class ContextHolder<T> extends WeakReference<T> {
    public ContextHolder(T r) {
        super(r);
    }

    /**
     * 判断是否存活
     *
     * @return
     */
    public boolean isAlive() {
        T ref = get();
        if (ref == null) {
            return false;
        } else {
            if (ref instanceof Service) {
                return isServiceAlive((Service) ref);
            } else if (ref instanceof Activity) {
                return isActivityAlive((Activity) ref);
            } else if (ref instanceof Fragment) {
                return isFragmentAlive((Fragment) ref);
            } else if (ref instanceof android.support.v4.app.Fragment) {
                return isV4FragmentAlive((android.support.v4.app.Fragment) ref);
            } else if (ref instanceof View) {
                return isContextAlive(((View) ref).getContext());
            } else if (ref instanceof Dialog) {
                return isContextAlive(((Dialog) ref).getContext());
            }
        }
        return true;
    }

    /**
     * 判断服务是否存活
     *
     * @param candidate
     * @return
     */
    boolean isServiceAlive(Service candidate) {
        if (candidate == null) {
            return false;
        }
        ActivityManager manager = (ActivityManager) candidate.getSystemService(Context.ACTIVITY_SERVICE);
        List<ActivityManager.RunningServiceInfo> services = manager.getRunningServices(Integer.MAX_VALUE);
        if (services == null) {
            return false;
        }
        for (ActivityManager.RunningServiceInfo service : services) {
            if (candidate.getClass().getName().equals(service.service.getClassName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * 判断activity是否存活
     *
     * @param a
     * @return
     */
    boolean isActivityAlive(Activity a) {
        if (a == null) {
            return false;
        }
        if (a.isFinishing()) {
            return false;
        }
        return true;
    }

    /**
     * 判断fragment是否存活
     *
     * @param fragment
     * @return
     */
    @TargetApi(Build.VERSION_CODES.HONEYCOMB_MR2)
    boolean isFragmentAlive(Fragment fragment) {
        boolean ret = isActivityAlive(fragment.getActivity());
        if (!ret) {
            return false;
        }
        if (fragment.isDetached()) {
            return false;
        }
        return true;
    }

    /**
     * 判断fragment是否存活
     *
     * @param fragment
     * @return
     */
    boolean isV4FragmentAlive(android.support.v4.app.Fragment fragment) {
        boolean ret = isActivityAlive(fragment.getActivity());
        if (!ret) {
            return false;
        }
        if (fragment.isDetached()) {
            return false;
        }
        return true;
    }

    /**
     * 判断是否存活
     *
     * @param context
     * @return
     */
    boolean isContextAlive(Context context) {
        if (context instanceof Service) {
            return isServiceAlive((Service) context);
        } else if (context instanceof Activity) {
            return isActivityAlive((Activity) context);
        }
        return true;
    }
}