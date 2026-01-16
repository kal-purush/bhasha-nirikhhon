package info.snoha.matej.linkeddatamap.app.gui.settings.items;

import android.content.Context;
import android.view.View;
import com.afollestad.materialdialogs.MaterialDialog;
import info.snoha.matej.linkeddatamap.Log;
import info.snoha.matej.linkeddatamap.R;
import info.snoha.matej.linkeddatamap.api.Layers;
import info.snoha.matej.linkeddatamap.app.gui.activities.SettingsActivity;
import info.snoha.matej.linkeddatamap.app.gui.settings.screens.LayerSettingsScreen;
import info.snoha.matej.linkeddatamap.app.gui.settings.screens.LayersSettingsScreen;
import info.snoha.matej.linkeddatamap.app.gui.settings.screens.SettingsScreenRegistry;
import info.snoha.matej.linkeddatamap.app.gui.utils.LayerLoaderUI;
import info.snoha.matej.linkeddatamap.app.gui.utils.UI;
import info.snoha.matej.linkeddatamap.app.internal.layers.Layer;
import info.snoha.matej.linkeddatamap.app.internal.layers.LayerDatabase;

import java.util.List;

public class AddLayerCloudSettingsItem extends AbstractSettingsItem {

    public AddLayerCloudSettingsItem(Context context) {
        super(context);
    }

    @Override
    public int getIcon() {
        return R.raw.cloud_download;
    }

    @Override
    public int getTitleResource() {
        return R.string.add_from_cloud;
    }

    @Override
    public String getSummary() {
        return null;
    }

    @Override
    public void onClick(View view) {

        LayerLoaderUI.loadFromCloud(getContext());
    }


}