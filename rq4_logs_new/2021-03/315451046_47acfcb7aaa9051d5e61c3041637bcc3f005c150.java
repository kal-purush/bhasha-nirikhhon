package com.example.pacmanlike.objects;

import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.Canvas;
import android.graphics.Paint;

import com.example.pacmanlike.R;
import com.example.pacmanlike.main.AppConstants;


class GhostSpritesID{
    public static int[][] getSpriteID(int id){
        int[][] tmp = new int[2][7];

        // vulnerable sprites
        tmp[1] = new int[]{
                R.drawable.ghost_vulnerable_0,
                R.drawable.ghost_vulnerable_1,
                R.drawable.ghost_vulnerable_2,
                R.drawable.ghost_vulnerable_3,
                R.drawable.ghost_vulnerable_4,
                R.drawable.ghost_vulnerable_5,
                R.drawable.ghost_vulnerable_6,
                R.drawable.ghost_vulnerable_7,
        };

        int[] ghost;

        switch (id){
            case 0:
                ghost = new int[]{
                        R.drawable.ghost_0_0,
                        R.drawable.ghost_0_1,
                        R.drawable.ghost_0_2,
                        R.drawable.ghost_0_3,
                        R.drawable.ghost_0_4,
                        R.drawable.ghost_0_5,
                        R.drawable.ghost_0_6,
                        R.drawable.ghost_0_7,
                };
                break;
            case 1:
                ghost = new int[]{
                        R.drawable.ghost_1_0,
                        R.drawable.ghost_1_1,
                        R.drawable.ghost_1_2,
                        R.drawable.ghost_1_3,
                        R.drawable.ghost_1_4,
                        R.drawable.ghost_1_5,
                        R.drawable.ghost_1_6,
                        R.drawable.ghost_1_7,
                };
                break;
            case 2:
                ghost = new int[]{
                        R.drawable.ghost_2_0,
                        R.drawable.ghost_2_1,
                        R.drawable.ghost_2_2,
                        R.drawable.ghost_2_3,
                        R.drawable.ghost_2_4,
                        R.drawable.ghost_2_5,
                        R.drawable.ghost_2_6,
                        R.drawable.ghost_2_7,
                };
                break;
            default:
                ghost = new int[]{
                        R.drawable.ghost_3_0,
                        R.drawable.ghost_3_1,
                        R.drawable.ghost_3_2,
                        R.drawable.ghost_3_3,
                        R.drawable.ghost_3_4,
                        R.drawable.ghost_3_5,
                        R.drawable.ghost_3_6,
                        R.drawable.ghost_3_7,
                };
                break;
        }
        tmp[0] = ghost;
        return  tmp;
    }
}



public class Ghost extends  DrawalbeObjects {

    private int[][] _spriteId;

    private int _id;

    public  Ghost(Context context, Vector startingPosition, int ghostID) {


        _id = ghostID;

        _frameIndex = 0;
        _totalframe = 20;
        _blockSize = AppConstants.getBlockSize();

        _direction = Direction.NONE;
        _prevDirection = Direction.UP;

        _position = new Vector(startingPosition.x*_blockSize + _blockSize/2, startingPosition.y*_blockSize + _blockSize/2);

        double tmp =  0.6 * AppConstants.getBlockSize();
        _objectSize = (int) tmp;

        _spriteId = GhostSpritesID.getSpriteID(ghostID);

        load(context);
    }


    @Override
    protected void load(Context context) {

        _sprites = new Bitmap[2][7];
        _sprites[0] = loadSprites(_spriteId[0], _objectSize, context);
        _sprites[1] = loadSprites(_spriteId[1], _objectSize, context);
    }

    @Override
    public void draw(Canvas canvas, Paint paint) {
        int div = 8;

        if(_frameIndex == _totalframe)
            _frameIndex =0;


        canvas.drawBitmap(_sprites[0][_frameIndex / 5], _position.x - _objectSize/2, _position.y - _objectSize / 2, paint);

        _frameIndex++;
    }

}