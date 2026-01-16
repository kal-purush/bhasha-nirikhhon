package github.com.foodactioncommitteecookbook;


import android.content.Context;
import android.graphics.Bitmap;
import android.net.Uri;
import android.provider.MediaStore;
import android.widget.ImageView;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BitmapLoader {
  private static final Logger log = LoggerFactory.getLogger(BitmapLoader.class);

  static public void loadImageViewBitmap (Context context, ImageView view, Uri bitmapUri) {

    try {
      Bitmap bitmap = MediaStore.Images.Media.getBitmap(context.getContentResolver(), bitmapUri);
      view.setImageBitmap(bitmap);

    } catch (Exception e) {
      log.warn("Unable to load image from Uri: {}.\n{}", bitmapUri, e);
    }
  }
}