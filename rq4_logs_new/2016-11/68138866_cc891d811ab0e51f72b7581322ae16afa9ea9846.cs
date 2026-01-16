using UnityEngine;
using System.Collections;
using UnityEngine.UI;
using UnityEngine.EventSystems;
using System.IO;
public class enternickname : MonoBehaviour,ISelectHandler {

    private InputField Input;
    private string s = "new Player has no name";
    TouchScreenKeyboard keyboard;
    public static string keyboardText = "";
    public string nick;

    void Start()
    {
       
    }

    public void OnSelect(BaseEventData eventData)
    {
        Debug.Log(this.gameObject.name + " was selected");
        keyboard = TouchScreenKeyboard.Open("", TouchScreenKeyboardType.Default, false, false, false, false);
        Debug.Log(gameObject.GetComponent<InputField>().text);
        Input = gameObject.GetComponent<InputField>();
        var se = new InputField.SubmitEvent();
        se.AddListener(SubmitName);
        Input.onEndEdit = se;
        
    }
    public void SubmitName(string arg0)
    {
        s = arg0;
        Debug.Log(s);     
        File.WriteAllText(@"Assets\Scripts\TextFile1.txt", s);

    }

   
    void OnMouseDown()
    {
        Debug.Log(this.gameObject.name + " was selected");
        keyboard = TouchScreenKeyboard.Open("", TouchScreenKeyboardType.Default, false, false, false, false);
        Debug.Log(gameObject.GetComponent<InputField>().text);
        Input = gameObject.GetComponent<InputField>();
        Debug.Log(Input);
    }

    void Update()
    {
        if (keyboard != null && keyboard.active == false)
        {
            if (keyboard.done == true)
            {
                keyboardText = keyboard.text;
                keyboard = null;
              
            }
        }
    }
}