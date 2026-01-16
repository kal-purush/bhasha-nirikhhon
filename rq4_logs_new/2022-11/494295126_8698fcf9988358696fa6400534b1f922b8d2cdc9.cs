using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using Cysharp.Threading.Tasks;
using UnityEngine;
using UnityEngine.Networking;

public static class APIClient
{
    public static async UniTask<List<string>> GetTargetGenerateFormulas(string serverHost)
    {
        string rawData;
        if (serverHost.Length != 0)
        {
            rawData = await GetTargetDataFromServer(serverHost);
        }
        else
        {
            rawData = GetTargetDataFromLocalData();
        }

        var rawListData = RawTextDataToFormulasList(rawData);
        return RandomlyPickFormulas(rawListData, 8);
    }

    private static async UniTask<string> GetTargetDataFromServer(string serverHost)
    {
        var rawText = string.Empty;
        var form = new WWWForm();

        var request = UnityWebRequest.Post(serverHost, form);
        await request.SendWebRequest();
        if (request.result == UnityWebRequest.Result.Success)
        {
            rawText = request.downloadHandler.text;
        }
        else
        {
            Debug.LogError(request.error);
        }
        return rawText;
    }
    
    private static string GetTargetDataFromLocalData()
    {
        return Resources.Load<TextAsset>("40smiles").text;
    }

    private static List<string> RawTextDataToFormulasList( string rawText )
    {
        var result = new List<string>();
        var sr = new StringReader(rawText);
        while (sr.Peek() >= 0)
        {
            result.Add(sr.ReadLine());
        }
        return result;
    }

    // TODO : Random하게 일정 숫자만 보여주는 기능은 추후에 빠질 수도 있을 것 같아 따로 함수로 빼둠.
    private static List<string> RandomlyPickFormulas( IReadOnlyList<string> data ,int pickNum )
    {
        var randomIndexes = new List<int>();
        while (randomIndexes.Count < pickNum)
        {
            var index = Random.Range(0, data.Count - 1);
            if (randomIndexes.Contains(index)) continue;
            randomIndexes.Add(index);
        }

        var result = new List<string>();
        foreach (var i in randomIndexes)
        {
            // TODO : 소문자와 대문자, 괄호의 의미가 있어서 추후에 유지하는 지에 대한 의문
            result.Add(Regex.Replace(data[i], @"[()23=#\[\]]", string.Empty).ToUpper());
        }
        return result;
    }
}