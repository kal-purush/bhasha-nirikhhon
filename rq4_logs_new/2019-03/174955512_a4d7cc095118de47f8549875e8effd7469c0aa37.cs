using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class ChainDisplay : MonoBehaviour
{
    const int AnimationFrame = 30;
    const int DurationAfterAnimation = 15;
    const float AnimDistance = 120;
    [SerializeField]
    Text numText;
    [SerializeField]
    Text chainText;
    Vector3 initPos;
    int chainNum;
    int animFrame;

    // Start is called before the first frame update
    void Start()
    {
        initPos = transform.localPosition;
        chainNum = 1;
        animFrame = 9999;
    }

    // Update is called once per frame
    void Update()
    {
        void UpdatePos(){
            if (animFrame >= AnimationFrame) return;
            var tmp = initPos;
            tmp.y = tmp.y + Func(animFrame, AnimationFrame) * AnimDistance;
            transform.localPosition = tmp;
        }

        void UpdateColor(){
            void SetColor(float val){
                var tmp = numText.color;
                tmp.a = val;
                numText.color = tmp;
                tmp = chainText.color;
                tmp.a = val;
                chainText.color = tmp;
            }

            if (animFrame >= AnimationFrame + DurationAfterAnimation){
                SetColor(0);
                return;
            }
            if (animFrame <= AnimationFrame){
                SetColor(1);
                return;
            }
            SetColor(1 - (animFrame - AnimationFrame)/(float)DurationAfterAnimation);
        }

        animFrame++;
        UpdatePos();
        UpdateColor();
    }

    public void PopUp(int chainNum){
        numText.text = chainNum.ToString();
        animFrame = 0;
    }

    private float Func(int frame, int totalFrame){
        if (frame <= 0) return 0;
        if (frame >= totalFrame) return 0;
        float x = frame/(float)totalFrame;
        return - x * x + x;
    }
}