package hm.orz.chaos114.android.circlewatcher.modules.main;

import android.content.Context;
import android.graphics.drawable.Drawable;
import android.support.annotation.DrawableRes;
import android.support.v4.content.ContextCompat;

import java.util.Locale;

import hm.orz.chaos114.android.circlewatcher.R;
import hm.orz.chaos114.android.circlewatcher.entity.Build;

public class BuildViewModel {
    private Context context;
    private Build build;

    BuildViewModel(Context context, Build build) {
        this.context = context;
        this.build = build;
    }

    public String getDisplayName() {
        return String.format(Locale.US, "%s / %s / %s #%d", build.getUsername(), build.getReponame(), build.getBranch(), build.getBuildNum());
    }

    public String getCommitMessage() {
        return build.getSubject();
    }

    public Drawable getStatusColor() {
        @DrawableRes int drawable;
        // :retried, :canceled, :infrastructure_fail, :timedout, :not_run, :running, :failed, :queued, :scheduled, :not_running, :no_tests, :fixed, :success
        switch (build.getStatus()) {
            case "retried":
                drawable = R.drawable.statusRetried;
                break;
            case "not_run":
                drawable = R.drawable.statusNotRun;
                break;
            case "running":
                drawable = R.drawable.statusRunning;
                break;
            case "failed":
                drawable = R.drawable.statusFailed;
                break;
            case "scheduled":
                drawable = R.drawable.statusScheduled;
                break;
            case "not_running":
                drawable = R.drawable.statusNotRunning;
                break;
            case "success":
                drawable = R.drawable.statusSuccess;
                break;
            default:
                return null;
        }
        return ContextCompat.getDrawable(context, drawable);
    }
}