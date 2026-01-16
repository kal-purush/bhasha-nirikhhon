using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
public enum WindowIndex
{
    FieldMenu,
    Max,
}
public class WindowManager : SingleToneBase<WindowManager>
{
    [SerializeField] GameObject UIWindowRoot;
    [SerializeField] UIFieldMenu fieldMenu;

    Dictionary<WindowIndex, string> windowPathDict = new Dictionary<WindowIndex, string>()
    {
        {WindowIndex.FieldMenu,"UI/Window/Part_FieldMenu/FieldMenu"},
    };

    //サイズ分確保
    List<WindowBase> windowList;//= new List<WindowBase>();

    void Start()
    {
        ChecktInitWindowList();
        windowList[(int)WindowIndex.FieldMenu] = fieldMenu;
    }
    public void ChecktInitWindowList()
    {
        if (windowList == null)
        {
            InitWindowList();
        }
    }
    void InitWindowList()
    {
        windowList = new List<WindowBase>();

        for (int idx = 0, idMax = (int)(WindowIndex.Max); idx < idMax; idx++)
        {
            windowList.Add(null);
        }
    }
    public static void CreateOpenWindow(WindowIndex index, Action<WindowBase> onLoad)
    {
        if (WindowManager.Get() == null) { return; }
        WindowManager.Get()._CreateOpenWindow(index, onLoad);
    }
    public void _CreateOpenWindow(WindowIndex index, Action<WindowBase> onLoad)
    {
        if (UIWindowRoot == null)
        {
            Debug.LogError("there is no root for Window UI");
            return;
        }

        if (windowList[(int)index] == null)
        {

            //GameObject prefab = AssetDatabase.LoadAssetAtPath<GameObject>(windowPathDict[index]);
            var loadPath = windowPathDict[index];
            GameObject prefab = Resources.Load<GameObject>(windowPathDict[index]);

            if (prefab == null)
            {
                Debug.LogError("could not find window Prefab " + index.ToString());
                return;
            }

            var windowInstance = GameObject.Instantiate(prefab, UIWindowRoot.transform).GetComponent<WindowBase>();

            if (windowInstance == null)
            {
                Debug.LogError("could not fine WIndowBase in " + index.ToString());
                return;
            }

            windowList[(int)index] = windowInstance;
        }
        else
        {
            if (windowList[(int)index].gameObject.activeSelf == true)
            {
                Debug.LogWarning(" window is active " + index.ToString());
            }
        }

        windowList[(int)index].Open();

        if (onLoad != null)
        {
            onLoad(windowList[(int)index]);
        }
    }
    public static bool IsWindowActive(WindowIndex index)
    {
        if (WindowManager.Get() == null) { return false; }
        return WindowManager.Get()._IsWindowActive(index);
    }
    public bool _IsWindowActive(WindowIndex index)
    {
        try
        {
            var w = windowList[(int)index];

            if (w == null) { return false; }

            return w.gameObject.activeInHierarchy;
        }
        catch
        {
            return false;
        }
    }
    public static WindowBase GetWindow(WindowIndex index)
    {
        if (WindowManager.Get() == null) { return null; }
        return WindowManager.Get()._GetWindow(index);
    }
    public WindowBase _GetWindow(WindowIndex index)
    {
        try
        {
            return windowList[(int)index];
        }
        catch
        {
            return null;
        }
    }
    public static void CloseWindow(WindowIndex index)
    {
        if (WindowManager.Get() == null) { return; }
        WindowManager.Get()._CloseWindow(index);
    }
    public void _CloseWindow(WindowIndex index)
    {
        if (windowList[(int)index] == null)
        {
            Debug.LogWarning("could not find wnd Instace when close " + index.ToString());
            return;
        }

        windowList[(int)index].Close();
    }
    public void ClearAll()
    {
        foreach (var wnd in windowList)
        {
            GameObject.Destroy(wnd.gameObject);
        }
        InitWindowList();
    }
}