package com.beeselmane.testapplication;

import android.app.Activity;
import android.content.Context;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.content.pm.ResolveInfo;
import android.net.Uri;
import android.os.Bundle;
import android.os.PersistableBundle;
import android.widget.Toast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class GlobalApplicationState
{
    public static final boolean showsAllApplications = false;
    public static GlobalApplicationState state = null;

    private List<AppPackage> applications = null;
    private Context context = null;

    public final class ApplicationStateChangedAction extends Activity
    {
        private GlobalApplicationState state = null;

        @Override
        public void onCreate(Bundle savedInstanceState, PersistableBundle persistentState)
        {
            super.onCreate(savedInstanceState, persistentState);
            Intent reason = this.getIntent();
            Uri dataURI = reason.getData();

            if (reason.getAction().equals(Intent.ACTION_PACKAGE_ADDED)) {
                System.out.println("Install: " + dataURI.toString());
            } else if (reason.getAction().equals(Intent.ACTION_PACKAGE_REMOVED)) {
                System.out.println("Uninstall: " + dataURI.toString());
            } else {
                System.err.println("ApplicationStateChangedAction created with invalid reason '" + reason.getAction() + "'");
                Toast.makeText(state.context, "Launcher Error!", Toast.LENGTH_SHORT).show();
            }
        }
    }

    public GlobalApplicationState(Context context)
    {
        if (GlobalApplicationState.state != null)
        {
            this.applications = GlobalApplicationState.state.applications;
            this.context = GlobalApplicationState.state.context;

            System.err.println("Warning: GlobalApplicationState() constructor called more than once!");
            return;
        }

        PackageManager packageManager = context.getPackageManager();
        this.applications = new ArrayList<>();
        this.context = context;

        Intent intent = new Intent(Intent.ACTION_MAIN, null);
        if (!GlobalApplicationState.showsAllApplications) intent.addCategory(Intent.CATEGORY_LAUNCHER);
        List<ResolveInfo> availableActivities = packageManager.queryIntentActivities(intent, 0);

        for (ResolveInfo info : availableActivities)
        {
            AppPackage app = new AppPackage(packageManager, info);
            app.canOpenAsApplication = true;
            this.applications.add(app);
        }

        Collections.sort(this.applications, new Comparator<AppPackage>() {
            @Override
            public int compare(AppPackage lhs, AppPackage rhs) {
                return lhs.label.toString().compareTo(rhs.label.toString());
            }
        });

        GlobalApplicationState.state = this;
    }

    public List<AppPackage> installedApplications()
    {
        return this.applications;
    }
}