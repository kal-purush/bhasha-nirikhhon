using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using DG.Tweening;

public class CustomKeyboard : MonoBehaviour
{
    public string word;

    [SerializeField]
    private InputField emailInput;
    [SerializeField]
    private Text[] letters;
    private bool isCapital = true;//是否大写
    private bool isShow = false;

    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public void AlphaClick(string alpha)
    {
        word = word + alpha;
        SetInputFieldText();
    }

    void SetInputFieldText()
    {
        emailInput.text = word;
    }

    //显示键盘
    public void Show()
    {
        if (!isShow)
        {
            word = "";
            SetInputFieldText();
            //CapitalClick();
            GetComponent<RectTransform>().DOAnchorPosY(-420, 0.3f);
            isShow = true;
        }
    }

    //点击大小写键
    public void CapitalClick()
    {
        if (isCapital)
        {
            //变小写
            foreach(Text letter in letters)
            {
                string s = letter.text;
                letter.text = s.ToLower();
            }   
        }   else
        {
            //变大写
            foreach (Text letter in letters)
            {
                string s = letter.text;
                letter.text = s.ToUpper();
            }
        }
        isCapital = !isCapital;
    }

    //删除
    public void Delete()
    {
        Debug.Log(word.Length);
        if (word.Length > 0)
        {
            word = word.Substring(0, word.Length - 1);
            SetInputFieldText();
        }

    }

    public void ClearAll()
    {
        word = "";
        SetInputFieldText();
    }

    public void Enter()
    {
        GetComponent<RectTransform>().DOAnchorPosY(-1250, 0.3f);
        isShow = false;
    }
}