using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using UnityEngine;

public class ResultDataController : MonoBehaviour
{
    /// <summary>データ名前</summary>
    static string m_textName = "ResultData";
    /// <summary>リザルトデータ</summary>
    ResultData m_resultData = null;

    /// <summary>
    ///データ初期化する前
    /// </summary>
    [RuntimeInitializeOnLoadMethod()]
    static void BeforInit()
    {
        //データがあったら、
        try
        {
            using (var reader = new StreamReader(FileController.GetFilePath(m_textName))) { }
        }
        catch (FileNotFoundException ex)
        {
            Debug.Log($"{ex}のファイルが見つかりませんでした。ファイルを作ります");
            ResultData resultData = new ResultData(0, 0, 0);
            FileController.TextSave(m_textName, JsonUtility.ToJson(resultData));
        }
    }

    // Start is called before the first frame update
    void Start()
    {
        m_resultData = GetResultLoad;
    }

    /// <summary>
    /// ランキングを変更する
    /// </summary>
    /// /// <param name="resultTime">結果タイム</param>
    public void RankingReload(float resultTime)
    {
        if (resultTime >= m_resultData.firstPlace)
        {
            m_resultData.thirdPlace = m_resultData.secondPlace;
            m_resultData.secondPlace = m_resultData.firstPlace;
            m_resultData.firstPlace = resultTime;
            ResultSave(m_resultData);
        }
        else if (resultTime >= m_resultData.secondPlace)
        {
            m_resultData.thirdPlace = m_resultData.secondPlace;
            m_resultData.secondPlace = resultTime;
            ResultSave(m_resultData);
        }
        else if (resultTime >= m_resultData.thirdPlace)
        {
            m_resultData.thirdPlace = resultTime;
            ResultSave(m_resultData);
        }
    }

    /// <summary>
    /// 結果セーブ
    /// </summary>
    /// <param name="resultData">結果データ</param>
    public void ResultSave(ResultData resultData)
    {
        FileController.TextSave(m_textName, JsonUtility.ToJson(resultData));
    }

    /// <summary>
    /// 結果ロードして返す
    /// </summary>
    public ResultData GetResultLoad
    {
        get
        {
            ResultData resultData = JsonUtility.FromJson<ResultData>(FileController.TextLoad(m_textName));
            Debug.Log(resultData);
            return resultData;
        }
    }
}

/// <summary>
/// 結果データ(Json)形式
/// </summary>
[Serializable]
public class ResultData
{
    /// <summary>１位</summary>
    public float firstPlace;
    /// <summary>２位</summary>
    public float secondPlace;
    /// <summary>３位</summary>
    public float thirdPlace;
    public ResultData(float first, float second, float third)
    {
        firstPlace = first;
        secondPlace = second;
        thirdPlace = third;
    }
}