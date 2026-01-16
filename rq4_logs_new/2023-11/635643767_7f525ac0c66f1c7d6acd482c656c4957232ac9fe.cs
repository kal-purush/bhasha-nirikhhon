using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ShowToolTip : MonoBehaviour
{
    [SerializeField] private string itemName;
    [SerializeField] private string itemDescription;

    private ToolTip toolTipMenu;
    private void OnEnable()
    {
        toolTipMenu = new ToolTip(itemName,itemDescription);
    }
    private void Update()
    {
        if(SceneControlManager.Instance.CurrentGameplayState == GameplayState.Pause)
        {
            toolTipMenu.ClearToolTip();
        }
    }
    private void OnTriggerEnter2D(Collider2D collision)
    {
        if (collision.CompareTag("Player") && collision.GetType().ToString() == Tags.BOXCOLLIDER2D)
        {
            toolTipMenu.SetToolTip();
        }
    }

    private void OnTriggerExit2D(Collider2D collision)
    {
        if (collision.CompareTag("Player") && collision.GetType().ToString() == Tags.BOXCOLLIDER2D)
        {
            toolTipMenu.ClearToolTip();
        }
    }
}