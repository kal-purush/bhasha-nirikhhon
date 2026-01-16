using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PanelManager : MonoBehaviour
{
    private Canvas currentCanvas;
    public Canvas MainCanvas;
    public Canvas optionsCanvas;
    public Canvas ConfirmationCanvas;

    private ArrayList OtherCanvas = new ArrayList();

    public void Quit()
    {
        Application.Quit();
    }

    public void Navigate(Canvas target)
    {
        this.OtherCanvas.Add(currentCanvas);
        this.currentCanvas = target;
        this.OtherCanvas.Remove(target);
    }
    
    // Start is called before the first frame update
    void Start()
    {
        this.currentCanvas = MainCanvas;
        this.currentCanvas.enabled = true;

        this.optionsCanvas.enabled = false;
        this.OtherCanvas.Add(optionsCanvas);
        this.ConfirmationCanvas.enabled = false;
        this.OtherCanvas.Add(ConfirmationCanvas);
    }

    // Update is called once per frame
    void Update()
    {
        this.currentCanvas.enabled = true;
        foreach(Canvas c in OtherCanvas)
        {
            c.enabled = false;
        }
    }
}