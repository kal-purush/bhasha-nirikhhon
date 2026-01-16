using System.Collections;
using System.Collections.Generic;
using UnityEngine;

/// <summary>
/// Controls all entityGUI components
/// </summary>
public class GameGUI : MonoBehaviour
{
    #region Variables

    //public GameObject EntityParent;
    public bool showGUI;
    private int lineLength = 20;

    #endregion


    #region Functions

    void Update()
    {
        if (Input.GetButtonDown("ShowGUI"))
        {
            Debug.Log("show GUI activated");
            showGUI = !showGUI;
        }
    }

    void OnGUI()
    {
        if (showGUI)
        {
            Entity[] entities = this.GetComponentsInChildren<Entity>();
            int height = 10;
            foreach (var entity in entities)
            {
               
                string text = entity.name + ": \n";
                int length = lineLength;

                if (entity.CurrentInstruction != null)
                {
                    text += entity.CurrentInstruction + "\n";
                    length += lineLength;
                }

                foreach (Instruction stackInt in entity.Instructions)
                {
                    text += stackInt + "\n";
                    length += lineLength;
                }

                GUI.TextArea(new Rect(10, height, 100, length), text, 200);
                height = height + length + 15;
            }
           
        }
    }

    #endregion
}