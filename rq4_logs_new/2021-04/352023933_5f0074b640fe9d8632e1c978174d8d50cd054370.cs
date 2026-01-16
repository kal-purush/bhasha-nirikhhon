using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System.IO;//ファイルシステムを使用するため

/// <summary>
/// ファイルを管理するクラス
/// </summary>
public class FileController
{
    /// <summary>
    /// ファイルセーブ （上書き）
    /// </summary>
    /// <param name="fileName">ファイル名前</param>
    public static void FileSave(string fileName, string text)
    {
        //指定されたファイルが存在しない場合、このパラメーターは無効であり、コンストラクターは新しいファイルを作成します。
        using (var writer = new StreamWriter(GetFilePath(fileName), append: false))
        {
            writer.Write(text);
        }
    }

    /// <summary>
    /// ファイルテキスト
    /// </summary>
    /// <param name="fileName">ファイル名前</param>
    /// <returns>ファイルの中身</returns>
    public static string LoadText(string fileName)
    {
        string text = "";
        try
        {
            using (var reader = new StreamReader(GetFilePath(fileName)))
            {
                while (!reader.EndOfStream)//テキストを行単位で読み込む　ReadToEndメソッドはまとめて読み込むのでメモリー消費が大きくなる　独習c＃p195 参照
                {
                    string line = reader.ReadLine();
                    //上書きの際元のもて見れるようにするため(消しても保存先は影響されない)
                    text += line;
                }

            }
        }
        catch (FileNotFoundException ex)
        {
            Debug.Log($"{ex}のファイルが見つかりませんでした。");
        }
        return text;
    }


    /// <summary>
    /// ファイルパスを返します。
    /// </summary>
    /// <returns>ファイルパス</returns>
    private static string GetFilePath(string fileName)
    {
        // Unity の場合はどこでもファイルの読み書きができるわけではないことに注意。Application.persistentDataPath を使って「読み書きできるところ」でファイル操作をすること。
        string filePath = Application.persistentDataPath + "/" + (fileName == "" ? Application.productName : fileName) + ".txt";
        return filePath;
    }
}