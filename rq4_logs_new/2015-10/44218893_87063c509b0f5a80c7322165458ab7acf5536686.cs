using UnityEngine;
using UnityEngine.UI;
using System.Collections;

public class TradeUIButtons : MonoBehaviour 
{
    //Give Buttons
    public Button tradeLumberPlus, tradeSheepPlus, tradeWheatPlus, tradeBrickPlus;
    public Button tradeLumberMinus, tradeSheepMinus, tradeWheatMinus, tradeBrickMinus;

    //Recieve Buttons
    public Button recieveLumberPlus, recieveSheepPlus, recieveWheatPlus, recieveBrickPlus;
    public Button recieveLumberMinus, recieveSheepMinus, recieveWheatMinus, recieveBrickMinus;

    //Special Buttons
    public Button sendRequest;
    public Button cancelButton;

    //The trade UI 
    public Canvas tradeCanvas;

    //Trade Plus Buttons
    #region
    public void OnTradeLumberPlus()
    {
        TradeUIController.tradeLumber++;
    }
    public void OnTradeSheepPlus()
    {
        TradeUIController.tradeSheep++;
    }
    public void OnTradeWheatPlus()
    {
        TradeUIController.tradeWheat++;
    }
    public void OnTradeBrickPlus()
    {
        TradeUIController.tradeBrick++;
    }
    #endregion

    //Trade Minus Buttons
    #region
    public void OnTradeLumberMinus()
    {
        if (TradeUIController.tradeLumber != 0)
        { 
            TradeUIController.tradeLumber--;
        }
    }
    public void OnTradeSheepMinus()
    {
        if (TradeUIController.tradeSheep != 0)
        {
            TradeUIController.tradeSheep--;
        }
    }
    public void OnTradeWheatMinus()
    {
        if (TradeUIController.tradeWheat != 0)
        {
            TradeUIController.tradeWheat--;
        }
    }
    public void OnTradeBrickMinus()
    {
        if (TradeUIController.tradeBrick != 0)
        {
            TradeUIController.tradeBrick--;
        }
    }
    #endregion

    //Recieve Plus Buttons
    #region
    public void OnRecieveLumberPlus()
    {
        TradeUIController.recieveLumber++;
    }
    public void OnRecieveSheepPlus()
    {
        TradeUIController.recieveSheep++;
    }
    public void OnRecieveWheatPlus()
    {
        TradeUIController.recieveWheat++;
    }
    public void OnRecieveBrickPlus()
    {
        TradeUIController.recieveBrick++;
    }
    #endregion

    //Recieve Minus Buttons
    #region
    public void OnRecieveLumberMinus()
    {
        if (TradeUIController.recieveLumber != 0)
        {
            TradeUIController.recieveLumber--;
        }
    }
    public void OnRecieveSheepMinus()
    {
        if (TradeUIController.recieveSheep != 0)
        {
            TradeUIController.recieveSheep--;
        }
    }
    public void OnRecieveWheatMinus()
    {
        if (TradeUIController.recieveWheat != 0)
        {
            TradeUIController.recieveWheat--;
        }
    }
    public void OnRecieveBrickMinus()
    {
        if (TradeUIController.recieveBrick != 0)
        {
            TradeUIController.recieveBrick--;
        }
    }
    #endregion

    //Send Request (Victor will need to set this method up)
    public void SendRequest()
    {
        Debug.Log("Sending Trade");
    }

    public void CancelTrade()
    {
        Debug.Log("Canceled");
        tradeCanvas.enabled = false;
    }
}