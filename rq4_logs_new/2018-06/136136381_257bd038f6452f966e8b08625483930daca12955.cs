using UnityEngine;
using UnityEditor;
using System.Collections;
using System.IO;
using System.Text;
using UnityEditorInternal;
using System.Linq;
using System;

public abstract class ConstanterBase {
    // 無効な文字を管理する配列
    private static readonly string[] INVALID_CHARACTERS =
    {
        " ", "!", "\"", "#", "$",
        "%", "&", "\'", "(", ")",
        "-", "=", "^",  "~", "\\",
        "|", "[", "{",  "@", "`",
        "]", "}", ":",  "*", ";",
        "+", "/", "?",  ".", ">",
        ",", "<"
    };

    public const string MENU_ITEM_ROOT = "Tools/Constanters/";

    public static bool CanCreate
    {
        get
        {
            return !EditorApplication.isPlaying && !EditorApplication.isCompiling && !Application.isPlaying;
        }
    }

    public static string RemoveInvalidCharacters(string str)
    {
        Array.ForEach(INVALID_CHARACTERS, s => str = str.Replace(s, string.Empty));
        return str;
    }

    public static string GetPath( string className)
    {
        var sb = new StringBuilder();
        return sb.AppendFormat("Assets/common/const/{0}.cs", className).ToString();
    }

    public static void Create( string className , string summary , string content)
    {
        CreateScript(WriteScript(className, summary, content), className);
    }

    private static void CreateScript( string text , string className)
    {
        var path = GetPath(className);
        var directoryName = Path.GetDirectoryName(path);
        if (!Directory.Exists(directoryName)) Directory.CreateDirectory(directoryName);

        File.WriteAllText(path, text, Encoding.UTF8);
        AssetDatabase.Refresh(ImportAssetOptions.ImportRecursive);
    }

    private static string WriteScript( string className , string summary , string content )
    {
        var sb = new StringBuilder();

        sb.AppendLine("/// <summary> ");
        sb.AppendLine("/// 自動生成で作られたコードです。");
        sb.AppendFormat("/// {0}" , summary).AppendLine();
        sb.AppendLine("/// </summary> ");

        sb.AppendFormat("public static class {0}", className);
        sb.AppendLine("{");

        sb.Append( content );

        sb.AppendLine("}");

        return sb.ToString();
    }
}