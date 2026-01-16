using System;
using System.Collections.Generic;
using System.Text;
using UnityEngine;

/// <summary>
/// 屏幕处理：android
/// </summary>
public class AndroidScreenHandler : IScreenHandler
{
    private Touch mOldTouch1;//触控点1
    private Touch mOldTouch2;//触控点2

    /// <summary>
    /// 初始化
    /// </summary>
    /// <param name="screenMoveRateX"></param>
    /// <param name="screenMoveRateY"></param>
    /// <param name="screenScaleChangeRate"></param>
    public AndroidScreenHandler(float screenMoveRateX, float screenMoveRateY, float screenScaleChangeRate) : base(screenMoveRateX, screenMoveRateY, screenScaleChangeRate)
    {
    }
    /// <summary>
    /// 屏幕缩放
    /// </summary>
    public override void ChangeScreenScale()
    {
        if (Input.touchCount <= 1) return;
       
        if (Input.touchCount >= 2)
        {
            if (Input.GetTouch(1).phase == TouchPhase.Began)
            {
                mOldTouch1 = Input.GetTouch(0);
                mOldTouch1 = Input.GetTouch(1);
                return;
            }
           
            float scaleFlag = Vector2.Distance(Input.GetTouch(0).position, Input.GetTouch(1).position) - Vector2.Distance(mOldTouch1.position, mOldTouch2.position);
            CameraScale(scaleFlag/100f);
            mOldTouch1 = Input.GetTouch(0);
            mOldTouch1 = Input.GetTouch(1);

        }
    }
   /// <summary>
   /// 屏幕移动
   /// </summary>
    public override void ScreenMove()
    {
        if (Input.touchCount <= 0) return;

        if (Input.touchCount==1)
        {
            if (Input.GetTouch(0).phase==TouchPhase.Began)
            {
                mOldTouch1 = Input.GetTouch(0);
                return;
            }

            mOldTouch2 = Input.GetTouch(0);
            CameraMove(mOldTouch1.position - mOldTouch2.position);
            mOldTouch1 = mOldTouch2;
           
        }
    }

}
