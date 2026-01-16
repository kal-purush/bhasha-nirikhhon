using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ScaleTransition : MonoBehaviour
{
    [SerializeField]
    RectTransform target;
    public int Size = 50;
    public bool StartWithBlackOut = false;
    public int TransitionFrame = 30;
    Vector3 targetVector;
    bool enlarge, shrink;
    int frame;
    // Start is called before the first frame update
    void Start()
    {
        var initSize = StartWithBlackOut ? Size : 0;
        target.localScale = new Vector3(initSize, initSize, initSize);
        targetVector = new Vector3(Size, Size, Size);
    }

    // Update is called once per frame
    void Update()
    {
        if (!enlarge || !shrink) return;
        frame++;
        if (enlarge)
        {
            transform.localScale = targetVector * Func(frame, TransitionFrame);
            return;
        }
        transform.localScale = targetVector * (1 - Func(frame, TransitionFrame));
    }

    public void Enlarge() => StartTransition(true);
    public void Shrink() => StartTransition(false);

    private float Func(int frame, int lastFrame)
    {
        var x = frame / (float)lastFrame;
        return x - x * x;
    }

    private void StartTransition(bool _enlarge)
        => (enlarge, shrink, frame) = (_enlarge, !_enlarge, 0);
}