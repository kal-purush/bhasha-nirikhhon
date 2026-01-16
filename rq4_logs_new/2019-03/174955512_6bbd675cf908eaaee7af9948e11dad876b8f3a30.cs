using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class TimePlusDisp : MoveUIBase
{
    Text text;
    readonly Vector3 moveDiff = new Vector3(0, 100, 0);
    protected override int AnimationFrame => 15;
    protected override int DurationAfterAnimation => 8;
    protected override Vector3 PositionDiff => moveDiff;
    protected override Color AlphaColor
    {
        get => text.color;
        set => text.color = value;
    }

    protected override void Start()
    {
        text = GetComponent<Text>();
        base.Start();
    }

    public void Pop(int time)
    {
        text.text = $"+{time}";
        PopUp();
    }

    protected override float Func(int frame, int totalFrame)
    {
        if (frame <= 0) return 0;
        if (frame >= totalFrame) return 0;
        float x = frame / (float)totalFrame;
        return -x * x + x;
    }
}