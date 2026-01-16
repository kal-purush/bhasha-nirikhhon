package com.nutrix.gamepad.app;

import android.content.Context;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.graphics.RectF;
import android.util.DisplayMetrics;
import android.view.MotionEvent;
import android.view.View;
import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import android.os.AsyncTask;

public class Field extends View {

    public  int      ROBOTS_NUM = 2;
    private RectF[]  robots;
    private int[]    coords;
    private Paint    roboPaint;
    private float    roboRadius;
    private int      robotSelected;
    private float    eX;
    private float    eY;
    private Client   netClient;
    private byte[]   messageToServer;
    private boolean  firstRun;

    public Field(Context content) {
        super(content);
        roboPaint = new Paint();
        roboPaint.setColor(Color.BLUE);

        DisplayMetrics dispM = getContext().getResources().getDisplayMetrics();
        roboRadius = dispM.heightPixels / 20;
        robotSelected = -1;
        eY         = (float)dispM.heightPixels / 480;
        eX         = (float)dispM.widthPixels / 640;
        netClient  = new Client();
        firstRun   = true;

        coords = new int[]{1, 0, 0, 2, 0, 0};
        robots = new RectF[ROBOTS_NUM];
        for (int i = 0; i < ROBOTS_NUM; i++)
            robots[i] = new RectF(0, 0, 10, 10);

        setFocusableInTouchMode(true);
        setClickable(true);
        setLongClickable(false);
    }


    @Override
    public void onDraw(Canvas canvas) {
        super.onDraw(canvas);
        if(firstRun) {
            netClient.execute();
            firstRun = false;
        }
        canvas.drawColor(Color.rgb(0, 139, 69));
        for(RectF r : robots)
            canvas.drawRoundRect(r, 150, 150, roboPaint);
    }

    @Override
    public boolean onTouchEvent(MotionEvent event) {
        float newX = event.getX();
        float newY = event.getY();
        if(event.getAction() == MotionEvent.ACTION_DOWN) {
            for(int i = 0; i < robots.length; i++) {
                if(robots[i].contains(newX, newY)) {
                    robotSelected = i;
                    return true;
                }
            }
        }
        if(robotSelected != -1) {
            robots[robotSelected].offsetTo(newX - 50, newY - 50);
            coords[robotSelected*3+1] = (int)(newX / eX);
            coords[robotSelected*3+2] = (int)(newY / eY);
            intParse(coords);
            netClient.toSend = true;
            invalidate(); // probably we won't need that
            //due to the server sending
            //messages to us constantly;
        }
        return false;
    }

    private void updateCanvas(String newCoords) {
        stringParse(newCoords);
        for(int i = 1; i < coords.length - 1; i += 3) {
            float left = coords[i]   * eX - roboRadius;
            float top  = coords[i+1] * eY - roboRadius;
            robots[(i-1)/3].left   = left;
            robots[(i-1)/3].right  = left + 2*roboRadius;
            robots[(i-1)/3].top    = top;
            robots[(i-1)/3].bottom = top + 2*roboRadius;
        }
        invalidate();
    }

    private void stringParse(String message) {
        String[] mesSplit = message.split(" ");
        for(int i = 0; i < coords.length; i++) {
            coords[i] = Integer.parseInt(mesSplit[i]);
        }
    }

    private void intParse(int[] coordinates) {
        StringBuilder res = new StringBuilder();
        for(int i = 0; i < coordinates.length - 2; i += 3) {
            res.append(coordinates[i]);
            res.append(" ");
            res.append(coordinates[i+1]);
            res.append(" ");
            res.append(coordinates[i+2]);
            res.append(" ");
        }
        res.deleteCharAt(res.length() - 1);
        messageToServer = res.toString().getBytes();
    }


    //==========================================================
    private class Client extends AsyncTask<Void, String, String> {
        private String  SERVER_IP   = "192.168.0.203";
        private int     SERVER_PORT = 8080;
        private boolean toSend      = false;

        @Override
        protected String doInBackground(Void... arg0) {
            Socket socket = new Socket();
            try {
                socket = new Socket(SERVER_IP, SERVER_PORT);
            } catch(UnknownHostException e) { e.printStackTrace(); }
              catch(IOException e) { e.printStackTrace(); }

            try {
                OutputStream sout = socket.getOutputStream();
                BufferedReader in  = new BufferedReader(
                        new InputStreamReader(socket.getInputStream()));
                while(true) {
                    if(toSend) sout.write(messageToServer);
                    publishProgress(in.readLine());
                }
            }
            catch (IOException e) { e.printStackTrace(); }
            return "";
        }

        @Override
        protected void onProgressUpdate(String... values) {
            updateCanvas(values[0]);
        }

        @Override
        protected void onPostExecute(String result) {  }
    }
}