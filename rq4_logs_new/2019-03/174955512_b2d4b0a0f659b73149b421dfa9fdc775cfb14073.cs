using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public abstract class MoveUIBase : MonoBehaviour
{
    protected abstract int AnimationFrame { get; }
    protected abstract int DurationAfterAnimation { get; }
    protected abstract Vector3 PositionDiff { get; }
    protected abstract Color[] AlphaColors { get; set; }

    Vector3 initPos;
    int animFrame;
    // Start is called before the first frame update
    protected virtual void Start()
    {
        initPos = transform.localPosition;
        animFrame = 999999;
    }

    // Update is called once per frame
    protected void Update()
    {
        void UpdatePos(int frame, int endFrame, Vector3 initialPos, Transform target, Vector3 posDiff)
        {
            if (frame >= endFrame) return;
            var tmp = initialPos;
            tmp = tmp + Func(frame, endFrame) * posDiff;
            target.localPosition = tmp;
        }

        Color UpdateColor(Color target)
        {
            Color MakeColor(Color tag, float val)
            {
                var tmp = tag;
                tmp.a = val;
                return tmp;
            }

            if (animFrame >= AnimationFrame + DurationAfterAnimation)
            {
                return MakeColor(target, 0);
            }
            if (animFrame <= AnimationFrame)
            {
                return MakeColor(target, 1);
            }
            return MakeColor(target, 1 - (animFrame - AnimationFrame) / (float)DurationAfterAnimation);
        }

        animFrame++;
        UpdatePos(animFrame, AnimationFrame, initPos, transform, PositionDiff);
        for (int i = 0; i < AlphaColors.Length; i++)
        {
            AlphaColors[i] = UpdateColor(AlphaColors[i]);
        }
    }

    protected abstract float Func(int frame, int totalFrame);
}