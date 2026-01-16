package com.example.its_a_feature_not_a_bug;


import android.content.res.Resources;
import android.graphics.Bitmap;
import android.util.Log;
import android.widget.ImageView;

import com.google.zxing.BarcodeFormat;
import com.google.zxing.WriterException;
import com.journeyapps.barcodescanner.BarcodeEncoder;

public class QRCodeGenerator {
    Event event;


    public static String generateQRCode(Event event) {
        /**
         *
         *
         *
         *
         */
        int screenWidth = Resources.getSystem().getDisplayMetrics().widthPixels;
        Bitmap qrImage = generateQR(event.getTitle(), screenWidth);

        return "QR Code for " + event.getTitle() + " has been generated!";
    }

    public static Bitmap generateQR(String content, int size) {
        Bitmap bitmap = null;
        try {
            BarcodeEncoder barcodeEncoder = new BarcodeEncoder();
            bitmap = barcodeEncoder.encodeBitmap(content, BarcodeFormat.QR_CODE, size, size);
        } catch (WriterException e) {
            Log.e("generateQR()", e.getMessage());
        }
        return bitmap;
    }


}