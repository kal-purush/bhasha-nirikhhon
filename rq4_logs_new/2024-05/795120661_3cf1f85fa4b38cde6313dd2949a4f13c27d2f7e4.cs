#nullable enable
using System.Linq;
using TMPro;
using UnityEditor;
using UnityEngine;
using UnityEngine.Assertions;
using UnityEngine.EventSystems;
using UnityEngine.Rendering.Universal;
using UnityEngine.UI;

/// <summary>
/// SerializeFieldがnullかどうかをチェックする拡張機能
/// </summary>
/// <remarks>
/// <para>Unity2022.2.8f1で動作確認。</para>
/// <para>
/// <item><description>ヒエラルキー上のオブジェクトに対して、nullであるSerializeFiledを持つ場合には、エラーのアイコンを表示し、エラーログも表示する</description></item>
/// <item><description>オブジェクトがたたみこまれている場合には、アイコンが表示されないことに注意</description></item>
/// </para>
/// </remarks>
public static class SerializeFieldNullChecker
{
     const int IconSize = 16;
     const string WarnIconName = "console.warnicon";
     const string ErrorIconName = "console.erroricon";
     static Texture? _warnIcon;
     static Texture? _errorIcon;

    [InitializeOnLoadMethod]
    static void Initialize()
    {
        Enable();

        /*
         * ビルトインアイコンの呼び出し方は以下を参考にした
         * https://qiita.com/Rijicho_nl/items/88e71b5c5930fc7a2af1
         * https://unitylist.com/p/5c3/Unity-editor-icons
         */
#pragma warning disable UNT0023 // Coalescing assignment on Unity objects
        _warnIcon ??= EditorGUIUtility.IconContent(WarnIconName).image;
        _errorIcon ??= EditorGUIUtility.IconContent(ErrorIconName).image;
#pragma warning restore UNT0023 // Coalescing assignment on Unity objects
    }

    static void Enable()
    {
        EditorApplication.hierarchyWindowItemOnGUI -= CheckNullReference;
        EditorApplication.hierarchyWindowItemOnGUI += CheckNullReference;
    }

    static void CheckNullReference(int instanceID, Rect selectionRect)
    {
        // instanceIDをオブジェクト参照に変換
        if (!(EditorUtility.InstanceIDToObject(instanceID) is GameObject gameObject)) return;

        // オブジェクトが所持しているコンポーネント一覧を取得
        var components = gameObject.GetComponents<MonoBehaviour>().Where(x => x != null);

        // Missingなコンポーネントが存在する場合はWarningアイコン表示
        foreach (var component in components)
        {
            if(IsIgnoreComponent(component)) continue;
            
            // SerializeFieldsにMissingなものが存在する場合はWarningアイコン表示
            var existsMissingField = ExistsNullField(component);
            if (existsMissingField)
            {
                Assert.IsNotNull(_errorIcon);
                DrawIcon(selectionRect, _errorIcon);
            }
        }
        
    }
    
    static bool IsIgnoreComponent(MonoBehaviour component)
    {
        if (component is UniversalAdditionalCameraData) return true;
        if (component is Light2D) return true;
        if (component is EventSystem) return true;
        if (component is TextMeshProUGUI) return true;
        if (component is Image) return true;
        if (component is Button) return true;
        if (component is Scrollbar) return true;
        if (component is ScrollRect) return true;
        return false;
    }
    
    static bool ExistsNullField(MonoBehaviour component)
    {
        using var serializedProp = new SerializedObject(component).GetIterator();

        while (serializedProp.NextVisible(true))
        {
            if (serializedProp.propertyType != SerializedPropertyType.ObjectReference) continue;
            if (serializedProp.objectReferenceValue != null) continue;

            //var fileId = serializedProp.FindPropertyRelative(PropertyNameOfFieldId);
            //if (fileId == null || fileId.intValue == 0) continue;

            Debug.LogError($"GameObject: { component.name}, Component: {component.GetType().Name}, Field: {serializedProp.propertyPath} is null.");
            return true;
        }

        return false;
    }

    static void DrawIcon(Rect selectionRect, Texture image)
    {
        var pos = selectionRect;
        pos.x = pos.xMax - IconSize;
        pos.width = IconSize;
        pos.height = IconSize;
        
        GUI.DrawTexture(pos, image, ScaleMode.ScaleToFit);
    }
}