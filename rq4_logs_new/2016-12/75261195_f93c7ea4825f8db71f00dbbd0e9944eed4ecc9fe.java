package com.example.hong.mylifelogger;

import android.app.Activity;
import android.app.AlertDialog;
import android.app.DatePickerDialog;
import android.app.TimePickerDialog;
import android.content.DialogInterface;
import android.content.Intent;
import android.database.sqlite.SQLiteDatabase;
import android.location.Address;
import android.location.Geocoder;
import android.net.Uri;
import android.os.Bundle;
import android.os.Environment;
import android.provider.MediaStore;
import android.view.View;
import android.widget.Button;
import android.widget.CheckBox;
import android.widget.CompoundButton;
import android.widget.DatePicker;
import android.widget.EditText;
import android.widget.ImageView;
import android.widget.Spinner;
import android.widget.TextView;
import android.widget.TimePicker;
import android.widget.Toast;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;

/**
 * Created by admin on 2016-12-06.
 */
public class AddEvent extends Activity{
    private static final int MARK_MANUAL_MAP = 0;
    private static final int PICK_FROM_ALBUM = 1;
    private static final int PICK_FROM_CAMERA =2;


    static EventDataBase dataBaseOpen;
    static SQLiteDatabase db;

    double latitude = 0;
    double longitude = 0;

    String addressString;
    int mYear, mMonth, mDay, mHour, mMinute;
    int sYear = 0;
    int sMonth= 0, sDay = 0, sHour= 0,sMinute= 0;
    private String dateString, timeString;
    private String  title, detail;


    // 카메라 관련
    private String url= "사진없음";
    private Uri mImageCaptureUri;
    private ImageView mPhotoImageView;
    private String folder = "Daily";
    private String filename;


    private MyLocation location; // 위치사용
    Spinner dailytype;      // 일상 스피너
    CheckBox locationcheck;
    CheckBox timecheck;
    Button locationbtn;
    Button timebtn;
    Button datebtn;
    Button completebtn;
    Button detailbtn;
    Button titlebtn;

    Button camerabtn;
    TextView picfileView;

    Intent intent;

    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_addevent);

        dataBaseOpen = new EventDataBase(this);
        db = dataBaseOpen.getWritableDatabase();


        locationcheck = (CheckBox) (findViewById(R.id.LocationCheck));
        timecheck =  (CheckBox) (findViewById(R.id.TimeCheck));
        locationbtn = (Button) findViewById(R.id.LocationHandle);
        datebtn = (Button) findViewById(R.id.datebtn);
        timebtn = (Button) findViewById(R.id.timebtn);
        completebtn = (Button) findViewById(R.id.completebtn);
        detailbtn = (Button) findViewById(R.id.alert2);
        titlebtn = (Button) findViewById(R.id.alert1);
        camerabtn = (Button) findViewById(R.id.albumbtn);

        picfileView = (TextView) findViewById(R.id.picfileView);
        //iv = (ImageView)findViewById(R.id.iv);


        GregorianCalendar cal = new GregorianCalendar();
        mYear = cal.get(Calendar.YEAR);
        mMonth = cal.get(Calendar.MONTH);
        mDay  = cal.get(Calendar.DAY_OF_MONTH);
        mHour = cal.get(Calendar.HOUR_OF_DAY);
        mMinute = cal.get(Calendar.MINUTE);

        intent = getIntent();


        // 카메라창
        findViewById(R.id.albumbtn).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                // TODO Auto-generated method stu

                AlertDialog.Builder alert = new AlertDialog.Builder(AddEvent.this).setTitle("사진 찍기");

                alert.setMessage(url);

                alert.setPositiveButton("사진촬영", new DialogInterface.OnClickListener() {
                    public void onClick(DialogInterface dialog, int whichButton) {
                        doTakePhotoAction();
                        //Toast.makeText(getApplicationContext(), url, Toast.LENGTH_SHORT).show();
                    }
                });
                alert.setNegativeButton("취소", new DialogInterface.OnClickListener() {
                    public void onClick(DialogInterface dialog, int whichButton) {
                        Toast.makeText(getApplicationContext(), url, Toast.LENGTH_SHORT).show();
                    }
                });

                alert.show();



            }
        });

        /*title 입력 하는 팝업창*/
        findViewById(R.id.alert1).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                // TODO Auto-generated method stub
                AlertDialog.Builder alert = new AlertDialog.Builder(AddEvent.this);

                alert.setTitle("제목 입력");
                alert.setMessage("Title 입력");
                final EditText Title = new EditText(AddEvent.this);
                alert.setView(Title);

                alert.setPositiveButton("ok", new DialogInterface.OnClickListener() {
                    public void onClick(DialogInterface dialog, int whichButton) {
                        title = Title.getText().toString();
                    }
                });
                alert.setNegativeButton("no", new DialogInterface.OnClickListener() {
                    public void onClick(DialogInterface dialog, int whichButton) {
                        title = "";
                    }
                });
                alert.show();

            }
        });

        /*detail 입력하는 팝업창*/
        findViewById(R.id.alert2).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                // TODO Auto-generated method stub
                AlertDialog.Builder alert = new AlertDialog.Builder(AddEvent.this);

                alert.setTitle("내용 입력");
                alert.setMessage("Detail입력");
                final EditText Detail = new EditText(AddEvent.this);
                alert.setView(Detail);

                alert.setPositiveButton("ok", new DialogInterface.OnClickListener() {
                    public void onClick(DialogInterface dialog, int whichButton) {
                        detail = Detail.getText().toString();
                    }
                });
                alert.setNegativeButton("no", new DialogInterface.OnClickListener() {
                    public void onClick(DialogInterface dialog, int whichButton) {
                        dialog.dismiss();                    }
                });
                alert.show();

            }
        });

        /*확인 버튼:  db에 데이터 입력*/
        findViewById(R.id.completebtn).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                // TODO Auto-generated method stub
                addressString = getAddress(latitude, longitude);
                //type = dailytype.getSelectedItem().toString();
               // String msg =title+ "\n" + detail +"\n" +type+"\n"+ dateString+"\n"+timeString+"\n"+addressString+"위도: " + latitude+ "경도: "+ longitude;
                //Toast.makeText(getApplicationContext(), msg, Toast.LENGTH_SHORT).show();

                insertData(dateString, timeString, addressString, latitude, longitude, title, detail, url);

                setResult(RESULT_OK, intent);

                finish();
            }
        });

        findViewById(R.id.datebtn).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                // TODO Auto-generated method stub
                new DatePickerDialog(AddEvent.this, dateSetListener, mYear, mMonth, mDay).show();
                dateString = Integer.toString(sYear) + "년 " + Integer.toString(sMonth) + "월 " + Integer.toString(sDay) + "일";

            }
        });

        findViewById(R.id.timebtn).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                // TODO Auto-generated method stub
                new TimePickerDialog(AddEvent.this, timeSetListener, mHour, mMinute, false).show();
                timeString = Integer.toString(sHour) + "시 " + Integer.toString(sMinute) + "분";

            }
        });

        findViewById(R.id.LocationHandle).setOnClickListener(new View.OnClickListener() {

            @Override
            public void onClick(View v) {
                // TODO Auto-generated method stub

                Intent mapIntent = new Intent(AddEvent.this, ManualMap.class);
                startActivityForResult(mapIntent, MARK_MANUAL_MAP);

            }
        });

        // 현재위치 체크박스 사용
        locationcheck.setOnCheckedChangeListener(new CompoundButton.OnCheckedChangeListener() {
            @Override
            public void onCheckedChanged(CompoundButton buttonView, boolean isChecked) {
                // TODO Auto-generated method stub
                if(isChecked){
                    locationbtn.setEnabled(false);
                    location =new MyLocation(AddEvent.this);
                    if (location.isGetLocation()) {
                        //위치
                        latitude = location.getLatitude();
                        longitude = location.getLongitude();
                        addressString = getAddress(latitude, longitude);
                        String msg = addressString+ "\n"+ "위도: "+latitude + "경도: " + longitude;
                        Toast.makeText(getApplicationContext(), msg, Toast.LENGTH_SHORT).show();

                    } else {
                        latitude = 0;
                        longitude = 0;
                        location.showSettingsAlert();
                    }
                }
                else{
                    locationbtn.setEnabled(true);
                }
            }
        });

        // 현재 시간 체크박스 사용
        timecheck.setOnCheckedChangeListener(new CompoundButton.OnCheckedChangeListener() {
            @Override
            public void onCheckedChanged(CompoundButton buttonView, boolean isChecked) {
                // TODO Auto-generated method stub
                if (isChecked) {
                    datebtn.setEnabled(false);
                    timebtn.setEnabled(false);
                    long now = System.currentTimeMillis();
                    Date date = new Date(now);
                    SimpleDateFormat CurDateFormat = new SimpleDateFormat("yyyy년 MM월 dd일");
                    SimpleDateFormat CurTimeFormat = new SimpleDateFormat("HH시 mm분");
                    dateString = CurDateFormat.format(date);
                    timeString = CurTimeFormat.format(date);

                } else {
                    datebtn.setEnabled(true);
                    timebtn.setEnabled(true);
                }

            }
        });


    }

    private void doTakePhotoAction()
    {
        Intent intent = new Intent(MediaStore.ACTION_IMAGE_CAPTURE);


        //저장할 파일 설정
        //외부 저장소 경로
        filename = title+"_"+dateString;
        String path = Environment.getExternalStorageDirectory().getAbsolutePath();

        //폴더명 및 파일명
        //String folderPath = path + File.separator + folder;


        //  File.separator는  / 슬래시 표시와 같다
        url = path + File.separator + folder + File.separator +  filename + ".jpg";


        // 저장 폴더 지정 및 폴더 생성
        //File fileFolderPath = new File(folderPath);
        //fileFolderPath.mkdir();


        mImageCaptureUri = Uri.fromFile(new File( url));


        // 임시로 사용할 파일의 경로를 생성
        //url = "tmp_" + dateString+" "+ timeString + ".jpg";
        //mImageCaptureUri = Uri.fromFile(new File(Environment.getExternalStorageDirectory(), url));

        intent.putExtra(android.provider.MediaStore.EXTRA_OUTPUT, mImageCaptureUri);
        // 특정기기에서 사진을 저장못하는 문제가 있어 다음을 주석처리 합니다.
        //intent.putExtra("return-data", true);
        startActivityForResult(intent, PICK_FROM_CAMERA);
    }



    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        super.onActivityResult(requestCode, resultCode, data);
        if(resultCode != RESULT_OK){
            return;
        }
        switch (requestCode){
            case MARK_MANUAL_MAP:
            {
                latitude = data.getDoubleExtra("LATITUDE_KEY",0.00);
                longitude = data.getDoubleExtra("LONGITUDE_KEY",0.00);
            }
            case PICK_FROM_CAMERA:
            {
                picfileView.setText(url);
                Toast.makeText(getApplicationContext(), url, Toast.LENGTH_SHORT).show();

            }
        }


    }

    private DatePickerDialog.OnDateSetListener dateSetListener = new DatePickerDialog.OnDateSetListener() {

        @Override
        public void onDateSet(DatePicker view, int year, int monthOfYear,
                              int dayOfMonth) {
            // TODO Auto-generated method stub
            sYear = year;
            sMonth = monthOfYear+1;
            sDay = dayOfMonth;
            dateString = Integer.toString(sYear) + "년 " + Integer.toString(sMonth) + "월 " + Integer.toString(sDay) + "일";
        }
    };

    private TimePickerDialog.OnTimeSetListener timeSetListener = new TimePickerDialog.OnTimeSetListener() {

        @Override
        public void onTimeSet(TimePicker view, int hourOfDay, int minute) {
            // TODO Auto-generated method stub
            sHour = hourOfDay;
            sMinute = minute;
            timeString = Integer.toString(sHour) + "시 " + Integer.toString(sMinute) + "분";

        }
    };


    public void insertData(String date, String time,
                           String address,  double latitude, double longitude, String title, String detail, String picturekey) {
        db.execSQL("INSERT INTO e_table "
                + "VALUES(NULL, '" + date
                + "', '" + time
                + "', '" + address
                + "', '" + latitude
                + "', '" + longitude
                + "', '" + title
                + "', '" + detail
                + "', '" + picturekey
                + "');");
    }

    public String getAddress(double latitude, double longitude) {
        String str = "주소를 찾는 중 입니다.";

        Geocoder geocoder = new Geocoder(this, Locale.getDefault());
        List<Address> list;
        try {
            if (geocoder != null) {
                list = geocoder.getFromLocation(latitude, longitude, 1);
                if (list != null && list.size() > 0) {
                    str = list.get(0).getAddressLine(0).toString();
                }
                return str;
            }
        } catch (IOException e) {
            return "error";
        }
        return str;
    }

}