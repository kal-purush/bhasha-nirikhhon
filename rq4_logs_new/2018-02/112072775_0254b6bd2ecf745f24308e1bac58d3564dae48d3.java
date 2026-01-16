package uk.ac.brighton.rlr17uni.inspirgram;

import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.BitmapShader;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.graphics.Shader;

/**
 * Created by rushlet on 06/02/2018.
 */

public class HighlightTransformation implements com.squareup.picasso.Transformation {
    private final Context mContext;

    // margin is the board in dp
    public HighlightTransformation(final Context context) {
        this.mContext = context;
    }

    @Override
    public Bitmap transform(final Bitmap source) {


        final Paint paint = new Paint();
        paint.setAntiAlias(true);
        paint.setShader(new BitmapShader(source, Shader.TileMode.CLAMP, Shader.TileMode.CLAMP));

        Bitmap output = Bitmap.createBitmap(source.getWidth(), source.getHeight(), Bitmap.Config.ARGB_8888);
        Canvas canvas = new Canvas(output);
        canvas.drawRect(0, 0, source.getWidth(), source.getHeight(), paint);

        if (source != output) {
            source.recycle();
        }

        Paint paint1 = new Paint();
        paint1.setColor(mContext.getResources().getColor(R.color.colorAccent));
        paint1.setStyle(Paint.Style.STROKE);
        paint1.setAntiAlias(true);
        paint1.setStrokeWidth(10);
        canvas.drawRect(0, 0, source.getWidth(), source.getHeight(), paint1);


        return output;
    }

    @Override
    public String key() {
        return "rounded";
    }
}