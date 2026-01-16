using UnityEngine;
using System.Collections;

public class CodexButton : MonoBehaviour {


    private CodexHandler codexHandler;
	private string nameToDisplay;
    private string nameOfFile;

    public void SetHandler(CodexHandler handler)
    {
        codexHandler = handler;
    }
    public void SendContents()
    {
        //check if anyone is subscribed to the event, if not, skip this

        //load filename stored on this button to text handler
        codexHandler.LoadContents(nameOfFile);
    }
}