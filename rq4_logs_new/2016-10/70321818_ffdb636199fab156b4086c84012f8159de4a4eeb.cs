using UnityEngine;
using System.Collections;
using System.Text;
using System;
using System.IO;

/// <summary>
/// イベントをテキストから読み込むクラス
/// </summary>
/// <remarks>
/// EventRepositoryからtagを参照してEventManagerに格納する役割
/// </remarks>
public class EventLoader : MonoBehaviour
{
    [SerializeField]
    EventRepository eventrepository = null;

    [SerializeField]
    MapChipController mapcontroller = null;

    string loadtext = "";

    // TODO: データ読み込みもっとスマートなやり方あるかも
    void Start()
    {
        //var testdata = Resources.Load<TextAsset>("MapData/EventData");


        readFileData();
        // 一行ずつ読む
        StringReader sr = new StringReader(loadtext);
        //空白を飛ばす
        var stArrayData = sr.ReadLine().Split(' ');

        // 一行表示
        int count = 0;
        int x = 0, y = 0;
        // block一個一個にeventを設定していく
        foreach (var stData in stArrayData)
        {
            if (count == 0) x = int.Parse(stData);
            if (count == 1) y = int.Parse(stData);
            if (count == 2)
            {
                mapcontroller.blocks[y, x].GetComponent<Block>().
                    event_manager.addEvent(
                    eventrepository.getEvent(stData));
                
            }
            count++;
        }
        count = 0;



    }

    /// <summary>
    /// テキスト読み込み関数
    /// </summary>
    /// http://qiita.com/WassyPG/items/5e4d3df219bba2a14f81
    void readFileData()
    {
        var fi = new FileInfo(Application.dataPath + "/GameMain/Resources/MapData/EventData.txt");

        try
        {
            using (StreamReader sr = new StreamReader(fi.OpenRead(), Encoding.UTF8))
            {
                loadtext = sr.ReadToEnd();
            }
        }
        catch (Exception e)
        {
            loadtext += SetDefaultText();
        }

    }
    string SetDefaultText()
    {
        return "\n";
    }

    // Update is called once per frame
    void Update()
    {

    }
}