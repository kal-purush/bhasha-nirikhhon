using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Events;


public class Test : MonoBehaviour
{
    private void Start()
    {
        EventDelegateExample.OnUnityAnswers += DoSomething;


    }

    void DoSomething(string message)
    {

    }
}

public class EventDelegateExample : MonoBehaviour
{
    // Callback signature
    // Has: Return of type 'void' and 1 parameter of type 'string'
    public delegate void AnswerCallback(string message);

    // Event declaration
    public static event AnswerCallback OnUnityAnswers;


    //public Action 
    public UnityAction<string> AnswerCallbackAction;

    public UnityEvent OnUnityAnswersEvent; 



    void Start()
    {
        // Whenever Unity Answers, call QuestionAnswered.
        OnUnityAnswers += QuestionAnswered;

        // Answer the question.
        RaiseAnswer("Great, someone made you a script example.");
        RaiseAnswer("Now, read the comments and play around with it.");
    }

    // Note: None of the following has to be 'public', they could also be 
    // 'private', 'protected', 'internal' or 'protected internal'.
    // But, let's keep it simple.    

    // This functions signature matches the callback signature! (Important)
    // Has: Return of type 'void' and 1 parameter of type 'string'
    public void QuestionAnswered(string message)
    {
        // Some dummy example code.
        Debug.Log(message);
    }



    // Calls, "raises" or "invokes" the event. Note that if no one subscribed
    // to the event, the event will be null. We need to check this first to
    // prevent errors.
    public void RaiseAnswer(string message)
    {
        if (OnUnityAnswers != null)
            OnUnityAnswers(message);
    }
}