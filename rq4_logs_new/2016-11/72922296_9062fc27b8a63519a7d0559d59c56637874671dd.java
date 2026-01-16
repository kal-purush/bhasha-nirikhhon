package ch.msengineering.budgetr;

import android.graphics.BitmapFactory;
import android.provider.ContactsContract;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.widget.ImageView;

/***
 * Shows the receipt of the expenditure on fullscreen.
 */
public class PictureViewActivity extends AppCompatActivity {

    /**
     * Path to the picture of the actual expenditure
     */
    String mPicturePath = "";

    /**
     * ImageView to show the picture in larger size.
     */
    ImageView mPictureFullScreenImageView;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_picture_view);

        //Get picture path of actual expenditure
        mPicturePath= getIntent().getExtras().getString("picturePath");

        //Show picture on screen
        mPictureFullScreenImageView = (ImageView) findViewById(R.id.iv_picture_fullscreen);
        mPictureFullScreenImageView.setImageBitmap(BitmapFactory.decodeFile(mPicturePath));
    }
}