package com.example.user.Activity;

import android.app.Dialog;
import android.graphics.Bitmap;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.widget.ImageView;
import android.widget.Toast;

import androidx.activity.EdgeToEdge;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.graphics.Insets;
import androidx.core.view.ViewCompat;
import androidx.core.view.WindowInsetsCompat;

import com.example.user.R;
import com.google.zxing.BarcodeFormat;
import com.google.zxing.MultiFormatWriter;
import com.google.zxing.WriterException;
import com.google.zxing.common.BitMatrix;
import com.journeyapps.barcodescanner.BarcodeEncoder;

public class ver_pedidoAfterPago extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        EdgeToEdge.enable(this);
        setContentView(R.layout.activity_ver_pedido);
        ViewCompat.setOnApplyWindowInsetsListener(findViewById(R.id.main), (v, insets) -> {
            Insets systemBars = insets.getInsets(WindowInsetsCompat.Type.systemBars());
            v.setPadding(systemBars.left, systemBars.top, systemBars.right, systemBars.bottom);
            return insets;
        });

        findViewById(R.id.btn_qr).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                showQRDialog();
            }
        });

    }

    private void showQRDialog() {

        Dialog dialog = new Dialog(this, R.style.BlurBackgroundDialog);
        MultiFormatWriter multiFormatWriter = new MultiFormatWriter();

        // Obtener el String enviado
        String pedidoDetalles = getIntent().getStringExtra("pedidoDetalles");

        // Inflar el diseño personalizado
        View dialogView = LayoutInflater.from(this).inflate(R.layout.custom_dialog_qr, null);
        dialog.setContentView(dialogView);

        // Configurar botones
        ImageView btn_close = dialogView.findViewById(R.id.btn_close);
        ImageView qr = dialogView.findViewById(R.id.generated_qr);


        try {
            BitMatrix bitMatrix = multiFormatWriter.encode(pedidoDetalles, BarcodeFormat.QR_CODE,600,600);
            BarcodeEncoder barcodeEncoder = new BarcodeEncoder();
            Bitmap bitmap = barcodeEncoder.createBitmap(bitMatrix);

            qr.setImageBitmap(bitmap);

        }catch (WriterException e) {

            throw new RuntimeException(e);

        }


        btn_close.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Toast.makeText(ver_pedidoAfterPago.this, "Cerrar qr", Toast.LENGTH_SHORT).show();
                dialog.dismiss();
            }
        });

        // Mostrar el diálogo
        dialog.show();


    }
}