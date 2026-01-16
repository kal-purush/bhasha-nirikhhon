package ar.com.siwca.coope.cooperativas.isma;

import android.os.Bundle;

import com.getcapacitor.BridgeActivity;
import com.getcapacitor.Plugin;

import java.util.ArrayList;
import com.getcapacitor.community.fcm.FCMPlugin;
public class MainActivity extends BridgeActivity {
  @Override
  public void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);

    // Initializes the Bridge
    this.init(savedInstanceState, new ArrayList<Class<? extends Plugin>>() {{
      add((Class<? extends Plugin>) com.capacitorjs.plugins.share.SharePlugin.class);
      add((Class<? extends Plugin>) com.capacitorjs.plugins.camera.CameraPlugin.class);
      add((Class<? extends Plugin>) com.capacitorjs.plugins.dialog.DialogPlugin.class);
      add((Class<? extends Plugin>) com.capacitorjs.plugins.filesystem.FilesystemPlugin.class);
      add((Class<? extends Plugin>) com.capacitorjs.plugins.network.NetworkPlugin.class);
      add((Class<? extends Plugin>) com.capacitorjs.plugins.app.AppPlugin.class);
      add(com.capacitorjs.plugins.pushnotifications.PushNotificationsPlugin.class);
      add(FCMPlugin.class);
    }});
  }
}