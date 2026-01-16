package airmonitor.com.androiddemo;

import android.content.Intent;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.net.Uri;
import android.os.Environment;
import android.provider.MediaStore;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.view.View.OnClickListener;
import android.widget.ImageView;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class CameraActivity extends AppCompatActivity {

    private static int REQUEST_THUMBNAIL = 1;// 请求缩略图信号标识
    private static int REQUEST_ORIGINAL = 2;// 请求原图信号标识
    private String sdPath;//SD卡的路径
    private String picPath;//图片存储路径
    private ImageView imgv; // 图片控件

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_camera);

        Button camera = (Button) findViewById(R.id.cameraBtn);
        imgv = (ImageView) findViewById(R.id.imagv);
        camera.setOnClickListener(cameraAction);
    }

    // 打开相机事件
    Button.OnClickListener cameraAction = new OnClickListener() {
        @Override
        public void onClick(View view) {

            Intent intent = new Intent(MediaStore.ACTION_IMAGE_CAPTURE);
            sdPath = Environment.getExternalStorageDirectory().getPath();
            picPath = sdPath + File.separator + "test.png";
            Uri uri = Uri.fromFile(new File(picPath));
            intent.putExtra(MediaStore.EXTRA_OUTPUT,uri);
            // 启动相机
            startActivityForResult(intent,REQUEST_ORIGINAL);
        }
    };

    @Override
    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        super.onActivityResult(requestCode, resultCode, data);
        if (resultCode == RESULT_OK){

            if (requestCode == REQUEST_ORIGINAL){

                // 获取到相机返回成功，拿到图片
                FileInputStream stream = null;
                try {
                    stream = new FileInputStream(picPath);
                    Bitmap bitmap = BitmapFactory.decodeStream(stream);
                    imgv.setImageBitmap(bitmap);
                }catch (FileNotFoundException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        stream.close();
                    } catch (IOException e){
                        e.printStackTrace();
                    }
                }

            }
        }
    }
}