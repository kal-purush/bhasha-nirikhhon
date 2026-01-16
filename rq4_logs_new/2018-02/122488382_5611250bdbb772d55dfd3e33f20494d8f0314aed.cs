using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System;
public static class CommonManager
{
    static public void OpenInputWindow(string title, Action<string> aOk, Action aCancel = null)
    {
        WindowManager.CreateOpenWindow(WindowIndex.CommonInoutWindow, (w) =>
        {
            var wnd = w as UICommonInputWIndow;
            wnd.Setup(title, aOk, aCancel);
        });
    }
}