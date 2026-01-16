package com.nvmvzv.kelimesoyle.recyclbinapp;

import android.content.Intent;
import android.graphics.Color;
import android.os.Bundle;
import android.text.Html;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ImageView;
import android.widget.TextView;
import android.widget.Toast;

import androidx.appcompat.app.AppCompatActivity;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLEncoder;

public class AccountActivity extends AppCompatActivity {




    int sayac = 0;

    String line;
    boolean netvar = true;
    BufferedReader reader;


    String xgonderveri1 = "";
    String xgonderveri2 = "";

    String gonderveri1 = "";
    String gonderveri2 = "";
    String gonderveri3 = "";
    String gonderveri4 = "";
    String gonderveri5 = "";
    String gonderveri6 = "";

    String AllData = "";

    String data = "";
    String dataX = "";

    public static String TCTCTC = "";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_account);

        Button btnkayit = (Button) findViewById(R.id.buttonKayit);
        btnkayit.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {

//////////////////////////////////////

                EditText eAdSoyad = (EditText) findViewById(R.id.editTextAdSoyad);
                String AdSoyad = eAdSoyad.getText().toString();


                EditText eTC = (EditText) findViewById(R.id.editTextTC);
                String TC = eTC.getText().toString();


                EditText eDogumYeri = (EditText) findViewById(R.id.editTextDogumYeri);
                String DogumYeri = eDogumYeri.getText().toString();


                EditText eDogumTarih = (EditText) findViewById(R.id.editTextDogumTarih);
                String DogumTarih = eDogumTarih.getText().toString();


                EditText eSifre = (EditText) findViewById(R.id.editTextSifre);
                String Sifre1 = eSifre.getText().toString();


                EditText eTelefon = (EditText) findViewById(R.id.editTextTelefon);
                String Telefon = eTelefon.getText().toString();


                if(!AdSoyad.equals("") && !TC.equals("") && !DogumYeri.equals("") && !Sifre1.equals("")){


                try {

                    gonderveri1 = AdSoyad.toString();
                    gonderveri2 = TC.toString();
                    gonderveri3 = Telefon.toString();
                    gonderveri4 = DogumYeri.toString();
                    gonderveri5 = DogumTarih.toString();
                    gonderveri6 = Sifre1.toString();

                    data = URLEncoder.encode("gonderveri1", "UTF-8") + "=" + URLEncoder.encode(gonderveri1.toString(), "UTF-8");
                    data +=  "&" + URLEncoder.encode("gonderveri2", "UTF-8") + "=" + URLEncoder.encode(gonderveri2.toString(), "UTF-8");
                    data +=  "&" + URLEncoder.encode("gonderveri3", "UTF-8") + "=" + URLEncoder.encode(gonderveri3.toString(), "UTF-8");
                    data +=  "&" + URLEncoder.encode("gonderveri4", "UTF-8") + "=" + URLEncoder.encode(gonderveri4.toString(), "UTF-8");
                    data +=  "&" + URLEncoder.encode("gonderveri5", "UTF-8") + "=" + URLEncoder.encode(gonderveri5.toString(), "UTF-8");
                    data +=  "&" + URLEncoder.encode("gonderveri6", "UTF-8") + "=" + URLEncoder.encode(gonderveri6.toString(), "UTF-8");


                    URL url = new URL("http://recyclbinapp.nvmvzv.com/kayitol.aspx");


                    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                    conn.setReadTimeout(100000);
                    conn.setConnectTimeout(100000);
                    conn.setRequestMethod("POST");
                    conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
                    conn.setRequestProperty("Content-Length", String.valueOf(data.length()));
                    conn.setDoInput(true);
                    conn.setDoOutput(true);


                    DataOutputStream out = new DataOutputStream(conn.getOutputStream());

                    out.writeBytes(data);

                    out.flush();

                    out.close();
                    // Defined URL  where to send data


                    reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
                    StringBuilder sb = new StringBuilder();

                    AllData = "";

                    while ((line = reader.readLine()).indexOf("</html") < 0) {

                        //sb.append(line);

                        AllData += line.toString();


                    }


                    String veriveri = AllData.toString().replace("@@@","æ");
                    String[] veriveriveri = veriveri.toString().split("æ");

                    if(veriveriveri.length > 0){

                            String Sonuc = veriveriveri[1].toString();


                            if(Sonuc.equals("kayitokey")){

                                Toast.makeText(AccountActivity.this, "KAYIT BAŞARIYLA YAPILDI", Toast.LENGTH_SHORT).show();

                            }
                            if(Sonuc.equals("kayiterror")){

                                Toast.makeText(AccountActivity.this, "HATA ! KAYIT YAPILAMADI", Toast.LENGTH_SHORT).show();

                            }
                            if(Sonuc.equals("kayitvar")){

                                Toast.makeText(AccountActivity.this, "DİKKAT ! DAHA ÖNCE KAYIT YAPTIRDINIZ", Toast.LENGTH_SHORT).show();

                            }
                    }
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                } catch (ProtocolException e) {
                    e.printStackTrace();
                } catch (MalformedURLException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                }else{


                    Toast.makeText(AccountActivity.this, "BOŞ ALANLARI DOLDURUNUZ !", Toast.LENGTH_SHORT).show();

                }

            } });

        Button btngiris = (Button) findViewById(R.id.buttonGiris);
        btngiris.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {

//////////////////////////////////////


                EditText eTC2 = (EditText) findViewById(R.id.editTextTC2);
                String TC2 = eTC2.getText().toString();


                EditText eSifre = (EditText) findViewById(R.id.editTextSifre2);
                String Sifre2 = eSifre.getText().toString();

                if(!TC2.equals("") && !Sifre2.equals("")) {

                    try {


                        xgonderveri1 = TC2.toString();
                        xgonderveri2 = Sifre2.toString();

                        dataX = URLEncoder.encode("gonderveri1", "UTF-8") + "=" + URLEncoder.encode(xgonderveri1.toString(), "UTF-8");
                        dataX += "&" + URLEncoder.encode("gonderveri2", "UTF-8") + "=" + URLEncoder.encode(xgonderveri2.toString(), "UTF-8");


                        URL url = new URL("http://recyclbinapp.nvmvzv.com/girisyap.aspx");


                        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                        conn.setReadTimeout(100000);
                        conn.setConnectTimeout(100000);
                        conn.setRequestMethod("POST");
                        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
                        conn.setRequestProperty("Content-Length", String.valueOf(dataX.length()));
                        conn.setDoInput(true);
                        conn.setDoOutput(true);


                        DataOutputStream out = new DataOutputStream(conn.getOutputStream());

                        out.writeBytes(dataX);

                        out.flush();

                        out.close();
                        // Defined URL  where to send data


                        reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
                        StringBuilder sb = new StringBuilder();

                        AllData = "";

                        while ((line = reader.readLine()).indexOf("</html") < 0) {

                            //sb.append(line);

                            AllData += line.toString();


                        }


                        String veriveri = AllData.toString().replace("@@@", "ß");
                        String[] veriveriveri = veriveri.toString().split("ß");

                        if (veriveriveri.length > 0) {

                            String Sonuc = veriveriveri[1].toString();

                            if (Sonuc.equals("girisok")) {

                                Toast.makeText(AccountActivity.this, "GİRİŞ BAŞARIYLA YAPILDI", Toast.LENGTH_SHORT).show();

                                AccountActivity.TCTCTC = TC2.toString();

                                Intent intent = new Intent(getApplicationContext(), DetailActivity.class);
                                startActivity(intent);


                            }

                            if (Sonuc.equals("girisfalse")) {

                                Toast.makeText(AccountActivity.this, "HATA ! ", Toast.LENGTH_SHORT).show();

                            }
                        }
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    } catch (ProtocolException e) {
                        e.printStackTrace();
                    } catch (MalformedURLException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }


                }else{

                    Toast.makeText(AccountActivity.this, "BOŞ ALANLARI DOLDURUNUZ !", Toast.LENGTH_SHORT).show();


                }


            } });



    }
}
