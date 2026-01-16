package github.nisrulz.qreader;

import android.content.Context;
import com.google.android.gms.vision.barcode.Barcode;
import com.google.android.gms.vision.barcode.BarcodeDetector;

/**
 * Created by punksta on 16.07.16.
 */
class BarcodeDetectorHolder {
  private static  BarcodeDetector detector;

  static BarcodeDetector getBarcodeDetector(Context context) {
    if (detector == null)
      detector = new BarcodeDetector.Builder(context.getApplicationContext()).setBarcodeFormats(Barcode.QR_CODE).build();
    return detector;
  }
}
