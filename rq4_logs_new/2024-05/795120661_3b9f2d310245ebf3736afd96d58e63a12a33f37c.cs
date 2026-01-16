#nullable enable
using System;
using UnityEngine;

namespace Daipan.Viewer.Scripts
{
    public class ViewerNumber
    {
        public int Number => Mathf.Max(IncreasedNumber - DecreasedNumber, 0); 
        int DecreasedNumber { get; set; }
        int IncreasedNumber { get; set; }

       public void IncreaseViewer(int amount)
       {
           if(amount < 0) Debug.LogWarning($"ViewerNumber.IncreaseViewer() amount is negative : {amount}");
           IncreasedNumber += amount;
       }
       
       public void DecreaseViewer(int amount)
       {
           if(amount < 0) Debug.LogWarning($"ViewerNumber.DecreaseViewer() amount is negative : {amount}");
           DecreasedNumber += amount;
       }
    }
}