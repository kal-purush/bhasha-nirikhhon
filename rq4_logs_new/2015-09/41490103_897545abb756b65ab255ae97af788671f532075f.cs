using UnityEngine;
using System.Collections;

/// <summary>
/// Хранилище звуковых файлов. Одиночка.
/// </summary>
public class AudioStorage : MonoBehaviour
{
    public static AudioStorage instance;
    
    /// <summary>Звук отскока</summary>
    public AudioClip rebound;
    /// <summary>Звук разрушения НЛО</summary>
    public AudioClip ufo;
    /// <summary>Звук разрушения игрока</summary>
    public AudioClip destroyPlayer;
    /// <summary>Звук фанфар при прохождении уровня</summary>
    public AudioClip fanfare;
    /// <summary>Звук разгона игрока</summary>
    public AudioClip impulse;
    /// <summary>Звуки для меню</summary>
    public AudioClip[] menu;
    /// <summary>Звуки при получении ачивки</summary>
    public AudioClip[] achievement;
    /// <summary>Звуки при взятии монетки</summary>
    public AudioClip[] coins;

    void Awake()
    {
        if (instance == null)
            instance = this;
        else if (instance != this)
            Destroy(gameObject);
    }
}