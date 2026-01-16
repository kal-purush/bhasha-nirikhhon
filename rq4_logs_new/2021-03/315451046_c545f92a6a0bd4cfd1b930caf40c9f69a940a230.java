package com.example.pacmanlike.gamelogic;

import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.util.DisplayMetrics;
import android.view.Display;
import android.view.WindowManager;

import com.example.pacmanlike.GameMap;
import com.example.pacmanlike.R;

public class AppConstants {


    static GameMap _gameMap;
    static GameEngine _engine;

    public static int SCREEN_WIDTH,
            SCREEN_HEIGHT;

    private static int MAP_SIZE_X = 7, MAP_SIZE_Y = 9;

    private static int _blockSize;

    private static int _center;

    public static Bitmap m;

    /**
     * Initiates the applciation constants
     * */
    public static void Initialization(Context context, GameMap map) {
        _gameMap = map;
        setScreenSize(context);

        _engine = new GameEngine(context);
        int a = R.drawable.basicmap;
        m = Bitmap.createScaledBitmap(BitmapFactory.decodeResource(context.getResources(), a), MAP_SIZE_X*_blockSize, MAP_SIZE_Y * _blockSize, false);
    }

    /**
     * Sets the size of one block according to the size of the device display
     */
    private static void setBlockSize() {
        int height = AppConstants.SCREEN_HEIGHT / AppConstants.MAP_SIZE_Y;
        int width = AppConstants.SCREEN_WIDTH / AppConstants.MAP_SIZE_X;

        _blockSize = height;

        if(height > width) {
            _blockSize = width;
        }
    }

    /**
     * Sets screen size constants accordingly to device screen size
     * */
    private static void setScreenSize(Context context) {
        WindowManager wm = (WindowManager) context.getSystemService(Context.WINDOW_SERVICE);
        Display display = wm.getDefaultDisplay();
        DisplayMetrics metrics = new DisplayMetrics();
        display.getMetrics(metrics);
        int width = metrics.widthPixels;
        int height = metrics.heightPixels;

        AppConstants.SCREEN_WIDTH = width;
        AppConstants.SCREEN_HEIGHT = height;

        setBlockSize();
    }

    /**
     * Returns the size of one block
     * @return block size
     */
    public static int getBlockSize() { return _blockSize; }

    /**
     * @return GameMap instance
     * */
    public static GameMap getGameMap() { return _gameMap; }


    /**
     * @return GameEngine instance
     * */
    public static GameEngine getEngine() { return _engine; }


    /**
     * Stops the given thread
     * @param thread
     * 			thread to stop
     * */
    public static void stopThread(Thread thread) {
        boolean retry = true;
        while (retry) {
            try {
                thread.join();
                retry = false;
            }
            catch (InterruptedException e) {}
        }
    }
}