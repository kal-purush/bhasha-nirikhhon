package com.android.quemeful_qr;

import static android.graphics.Color.BLACK;
import static android.graphics.Color.WHITE;

import android.graphics.Bitmap;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.ImageView;
import android.widget.Toast;

import androidx.appcompat.app.AppCompatActivity;
import androidx.appcompat.widget.Toolbar;

import com.google.android.material.textfield.TextInputEditText;
import com.google.zxing.BarcodeFormat;
import com.google.zxing.MultiFormatWriter;
import com.google.zxing.WriterException;
import com.google.zxing.common.BitMatrix;

/**
 * This is a new activity for Generating QR code.
 * This code is just a check if the QR code generates correctly.
 * For the project, this same piece of code maybe be used for both cases of generating new QR when,
 * new event is created and when event promotion generates a promotion QR.
 * Note: For the purpose of generating and scanning a QR code,
 * zxing library has been implemented in the build.graddle.kts(Module:app)
 */
public class GenerateNewQRActivity extends AppCompatActivity {
    private ImageView QRImage;
    private TextInputEditText event;
    private Button generate;

    /**
     * This onCreate method is only used to setOnClickListener to the generate button and displays the QR code as image.
     * The generation and appearance of the QR code is done in the createBitmap method below.
     *
     * @param savedInstanceState If the activity is being re-initialized after
     *     previously being shut down then this Bundle contains the data it most
     *     recently supplied in {@link #onSaveInstanceState}.  <b><i>Note: Otherwise it is null.</i></b>
     */
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_generate_new_qractivity);

        QRImage = findViewById(R.id.QR);
        event = findViewById(R.id.EnterText);
        generate = findViewById(R.id.generate);

        // navigating back to the previous activity
        Toolbar toolbar = (Toolbar) findViewById(R.id.backTool);

        toolbar.setNavigationOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                // back clicked
                finish();
            }
        });

        /**
         * References: URLs (Should specifically add Author, License, Published Date and Access Date: 21.02.2024)
         * https://www.youtube.com/watch?v=zHStZwXtbj0
         * https://www.youtube.com/watch?v=5DEHmN4PmA0
         * https://stackoverflow.com/questions/8831050/android-how-to-read-qr-code-in-my-application
         * https://stackoverflow.com/questions/28232116/android-using-zxing-generate-qr-code
         */
        generate.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String text = event.getText().toString();
                if (text.isEmpty()) {
                    Toast.makeText(GenerateNewQRActivity.this, "Please enter the Event Name for promotion", Toast.LENGTH_SHORT).show();
                } else {
                    Bitmap bitmap = createBitmap(text);
                    QRImage.setImageBitmap(bitmap);
                }
            }
        });
    }

    /**
     * Creates/Encodes the QR code using Bitmatrix
     * @param text Takes input text (e.g. the event name) to create a QR code based on that
     * @return Returns BitMap map
     */

    private Bitmap createBitmap(String text) {
        BitMatrix matrix;
        try {
            // encode the QR code
            matrix = new MultiFormatWriter().encode(text, BarcodeFormat.QR_CODE, 250, 250);
        }
        catch(WriterException e){
            e.printStackTrace();
            return null;
        }
        // appearance of QR Code
        int w = matrix.getWidth();
        int h = matrix.getHeight();
        int[] dim = new int[w * h];
        for(int i = 0; i < h ; i++){
            int offset = i * w;
            for(int j = 0; j < w; j++){
                dim[offset + j] = matrix.get(j,i) ? BLACK : WHITE;
            }
        }
        Bitmap map = Bitmap.createBitmap(w, h, Bitmap.Config.ARGB_8888);
        map.setPixels(dim, 0, w,0, 0, w, h);
        return map;
    }
}