package org.odk.collect.android.activities;

import static org.odk.collect.androidshared.ui.DialogFragmentUtils.showIfNotShowing;

import android.content.ActivityNotFoundException;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.os.Bundle;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.Button;

import androidx.appcompat.widget.Toolbar;

import org.odk.collect.android.R;
import org.odk.collect.android.activities.viewmodels.CurrentProjectViewModel;
import org.odk.collect.android.activities.viewmodels.MainMenuViewModel;
import org.odk.collect.android.injection.DaggerUtils;
import org.odk.collect.android.projects.ProjectIconView;
import org.odk.collect.android.projects.ProjectSettingsDialog;
import org.odk.collect.android.utilities.ApplicationConstants;
import org.odk.collect.android.utilities.ThemeUtils;
import org.odk.collect.androidshared.ui.multiclicksafe.MultiClickGuard;
import org.odk.collect.settings.SettingsProvider;

import javax.inject.Inject;

public class LandingPageActivity extends CollectAbstractActivity{
    //button
    private Button sondageButton;
    private Button messageButton;
    private Button articleButton;
    private Button centreHospitalButton;
    private Button cycleButton;

    @Inject
    MainMenuViewModel.Factory viewModelFactory;

    @Inject
    CurrentProjectViewModel.Factory currentProjectViewModelFactory;

    @Inject
    SettingsProvider settingsProvider;

    private MainMenuViewModel mainMenuViewModel;

    private CurrentProjectViewModel currentProjectViewModel;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        new ThemeUtils(this).setDarkModeForCurrentProject();

        super.onCreate(savedInstanceState);
        DaggerUtils.getComponent(this).inject(this);
        setContentView(R.layout.landing_page);

        initToolbar();

        // songade button
        sondageButton = findViewById(R.id.sondage);
        sondageButton.setText(getString(R.string.btn_sondage));
        sondageButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent i = new Intent(getApplicationContext(), MainMenuActivity.class);
                i.putExtra(ApplicationConstants.BundleKeys.FORM_MODE,
                        ApplicationConstants.FormModes.EDIT_SAVED);
                startActivity(i);
            }
        });

        // message button
        messageButton = findViewById(R.id.message);
        messageButton.setText(getString(R.string.btn_messsage));
        messageButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent i = new Intent(getApplicationContext(),
                        MainMenuActivity.class);
                startActivity(i);
            }
        });

        //article Button
        articleButton = findViewById(R.id.article);
        articleButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent i = new Intent(getApplicationContext(), MainMenuActivity.class);
                i.putExtra(ApplicationConstants.BundleKeys.FORM_MODE,
                        ApplicationConstants.FormModes.VIEW_SENT);
                startActivity(i);
            }
        });

//        // centre Hospital Button
        centreHospitalButton = findViewById(R.id.centre_Hospital);
        centreHospitalButton.setText(getString(R.string.btn_centreHospital));
        centreHospitalButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent i = new Intent(getApplicationContext(),
                        MainMenuActivity.class);
                startActivity(i);
            }
        });

        // cycle Button
        cycleButton = findViewById(R.id.cycle);
        cycleButton.setText(getString(R.string.btn_cycle));
        cycleButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                openPeriodical();
            }
        });
    }

    @Override
    protected void onResume() {
        super.onResume();
    }

    @Override
    public boolean onPrepareOptionsMenu(Menu menu) {
        final MenuItem projectsMenuItem = menu.findItem(R.id.projects);

        ProjectIconView projectIconView = (ProjectIconView) projectsMenuItem.getActionView();
        projectIconView.setOnClickListener(v -> onOptionsItemSelected(projectsMenuItem));
        projectIconView.setContentDescription(getString(R.string.projects));

        return super.onPrepareOptionsMenu(menu);
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        getMenuInflater().inflate(R.menu.main_menu, menu);
        return super.onCreateOptionsMenu(menu);
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        if (!MultiClickGuard.allowClick(getClass().getName())) {
            return true;
        }

        if (item.getItemId() == R.id.projects) {
            showIfNotShowing(ProjectSettingsDialog.class, getSupportFragmentManager());
            return true;
        }
        return super.onOptionsItemSelected(item);
    }

    private void initToolbar() {
        Toolbar toolbar = findViewById(R.id.toolbar);
        setSupportActionBar(toolbar);
    }

    private boolean openPeriodical() {
        String packageName = getString(R.string.btn_cycle);
        PackageManager manager = getApplicationContext().getPackageManager();
        try {
            Intent i = manager.getLaunchIntentForPackage(packageName);
            if (i == null) {
                return false;
                //throw new ActivityNotFoundException();
            }
            i.addCategory(Intent.CATEGORY_LAUNCHER);
            startActivity(i);
            return true;
        } catch (ActivityNotFoundException e) {
            return false;
        }
    }
}