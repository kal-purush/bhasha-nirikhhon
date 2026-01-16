package com.example.pacmanlike.objects.ghosts;

import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.Canvas;
import android.graphics.Paint;

import com.example.pacmanlike.R;
import com.example.pacmanlike.main.AppConstants;
import com.example.pacmanlike.objects.Direction;
import com.example.pacmanlike.objects.DrawalbeObjects;
import com.example.pacmanlike.objects.Vector;

public class Ghost extends DrawalbeObjects {

    private int[][] _spriteId;

    private int _id;

    private boolean _vulnerable;

    public  Ghost(Context context, Vector startingPosition, int ghostID) {


        _id = ghostID;

        _frameIndex = 0;
        _totalframe = 56;
        _frameCongruence = 8;



        _blockSize = AppConstants.getBlockSize();

        _direction = Direction.NONE;
        _prevDirection = Direction.UP;


        setRelativePosition(startingPosition);

        //_position = new Vector(startingPosition.x*_blockSize + _blockSize/2, startingPosition.y*_blockSize + _blockSize/2);

        double tmp =  0.6 * AppConstants.getBlockSize();
        _objectSize = (int) tmp;

        _spriteId = GhostID.getSpriteID(ghostID);

        load(context);

        _vulnerable=  false;
    }


    @Override
    protected void load(Context context) {

        _sprites = new Bitmap[2][7];
        _sprites[0] = loadSprites(_spriteId[0], _objectSize, context);
        _sprites[1] = loadSprites(_spriteId[1], _objectSize, context);
    }

    @Override
    public void draw(Canvas canvas, Paint paint) {

        if(_frameIndex == _totalframe)
            _frameIndex =0;

        if(_vulnerable) {
            canvas.drawBitmap(_sprites[1][_frameIndex / _frameCongruence], _position.x - _objectSize / 2, _position.y - _objectSize / 2, paint);
        }else {
            canvas.drawBitmap(_sprites[0][_frameIndex / _frameCongruence], _position.x - _objectSize / 2, _position.y - _objectSize / 2, paint);
        }
        _frameIndex++;
    }

}