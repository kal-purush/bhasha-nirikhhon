using System.Collections;
using System.Collections.Generic;
using TMPro;
using UnityEngine;
using UnityEngine.UI;

public class ConversationManger : MonoBehaviour
{
    public TMP_Text characterNameText; // 显示角色名字的文本框
    public TMP_Text dialogueText; // 显示对话的文本框
    public Image leftCharacterImage; // 显示左侧角色头像的图片框
    public Image rightCharacterImage; // 显示右侧角色头像的图片框
    public Button nextButton; // 下一个按钮
    public Canvas dialogueCanvas; // 对话框的 Canvas

    private Queue<string> sentences = new Queue<string>();// 对话内容队列

    private string currentCharacterName; // 当前对话角色名字
 

    void Start()
    {
        nextButton.onClick.AddListener(DisplayNextSentence);
    }

    public void StartConversation(Conversation conversation)
    {
        if (sentences == null)
        {
            sentences = new Queue<string>();
        }
        sentences.Clear();

        foreach (string sentence in conversation.xieYanSentences)
        {
            sentences.Enqueue(sentence);
        }

        // 添加右侧角色的对话内容和头像
        foreach (string sentence in conversation.xieMingSentences)
        {
            sentences.Enqueue(sentence);
        }



        DisplayNextSentence();
    }

    public void DisplayNextSentence()
    {
        if (sentences.Count == 0)
        {
            EndConversation();
            return;
        }

        string sentence = sentences.Dequeue();
        dialogueText.text = sentence;

        if (!string.IsNullOrEmpty(currentCharacterName))
        {
            characterNameText.text = currentCharacterName;
        }

        if (leftCharacterImage.gameObject.activeSelf)
        {
            rightCharacterImage.gameObject.SetActive(true);
            leftCharacterImage.gameObject.SetActive(false);
        }
        else if (rightCharacterImage.gameObject.activeSelf)
        {
            leftCharacterImage.gameObject.SetActive(true);
            rightCharacterImage.gameObject.SetActive(false);
        }
    }

    void EndConversation()
    {
        dialogueCanvas.gameObject.SetActive(false);
    }
}

[System.Serializable]
public class Conversation
{
    public string xieYanName = "谢言";
    public string[] xieYanSentences = {
        "纸鸢纸鸢，挂树上了！",
        "纸鸢飞高高，阿言也要高高！"
    };

    public string xieMingName = "谢明";
    public string[] xieMingSentences = {
        "言言别担心，阿姊来！",
        "言言以后一定会和大树一样高高的~"
    };
    
    public Sprite xieYanSprite;
    public Sprite xieMingSprite;
}