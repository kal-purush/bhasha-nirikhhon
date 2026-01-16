using System;
using UnityEngine;
public class MonoSingleton<T> : MonoBehaviour where T : Component
{
    private static T s_instance;

    public static T Instance
    {
        get { return CreateInstance();}
    }

    public static T CreateInstance()
    {
        if (s_instance == null)
        {
            Type typeFromHandle = typeof(T);
            s_instance = (T) FindObjectOfType(typeFromHandle);
            if (s_instance == null)
            {
                GameObject gameObject = new GameObject(typeof(T).Name);
                s_instance = gameObject.AddComponent<T>();
            }
        }

        return s_instance;
    }

    public static bool HasInstance()
    {
        return s_instance != null;
    }

    public static void DestroyInstance()
    {
        if (s_instance != null)
        {
            (s_instance as MonoSingleton<T>).UnInit();
            Destroy(s_instance.gameObject);
        }

        s_instance = null;
    }

    private void Awake()
    {
        if (s_instance != null && MonoSingleton<T>.s_instance.gameObject != base.gameObject)
        {
            if (Application.isPlaying)
            {
                Destroy(base.gameObject);
            }
            else
            {
                DestroyImmediate(base.gameObject);
            }
        }
        else
        {
            if (s_instance == null)
            {
                s_instance = base.GetComponent<T>();
            }
        }
        DontDestroyOnLoad(gameObject);
        this.Init();
    }

    private void OnDestroy()
    {
        if (s_instance != null && s_instance.gameObject == gameObject)
        {
            s_instance = null;
        }
    }

    public virtual void Init()
    {
        
    }

    public virtual void UnInit()
    {
        
    }
}