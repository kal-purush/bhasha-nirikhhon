using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class ScoreGetDisplay : MonoBehaviour
{
    const int AnimationFrame = 42;
    const float AnimDistance = 30;
    const int AnimationRate = 6;
    [SerializeField]
    Text text;
    Vector3 initPos;
    int animFrame;
    // Start is called before the first frame update
    void Start()
    {
        initPos = transform.localPosition;
        animFrame = 9999;
    }

    // Update is called once per frame
    void Update()
    {
        animFrame++;
        void UpdatePos(){
            var tmp = initPos;
            tmp.x = tmp.x + Func(animFrame, AnimationFrame) * AnimDistance;
            transform.localPosition = tmp;
        }

        void UpdateColor(){
            var tmp = text.color;
            tmp.a = 1 - Mathf.Abs(Func(animFrame, AnimationFrame));
            text.color = tmp;
        }

        UpdatePos();
        UpdateColor();
    }

    public void PopUp(int score){
        animFrame = 0;
        text.text = $"+{score}";
    }

    private float Func(int frame, int endFrame){
        int duration = endFrame / AnimationRate;
        if(frame >= endFrame) return 1;
        if (frame < duration){
            return ((float)frame / duration) - 1;
        }
        if (frame >= endFrame - duration){
            return ((float)frame / duration) - AnimationRate + 1;
        }
        return 0;
    }
}