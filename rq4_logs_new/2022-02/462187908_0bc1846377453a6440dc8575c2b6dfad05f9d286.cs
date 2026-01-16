
#if UNITY_EDITOR

#nullable enable

namespace Edanoue.SaveData
{
    using System;
    using System.Collections.Generic;

    using UnityEngine;
    using UnityEngine.SceneManagement;
    // エディタ関連ライブラリを使用する宣言
    using UnityEditor;

    // Odin のやつ
    using Sirenix.OdinInspector.Editor;
    using Sirenix.OdinInspector;
    using Sirenix.Utilities.Editor;
    using Sirenix.Utilities;

    /// <summary>
    ///
    /// </summary>
    public class OpeningSceneSaveDataWindow : OdinEditorWindow
    {
        [MenuItem("Edanoue/SaveData/OpeningSceneSaveDataWindow")]
        private static void OpenWindow()
        {
            var window = GetWindow<OpeningSceneSaveDataWindow>();

            // Nifty little trick to quickly position the window in the middle of the editor.
            window.position = GUIHelper.GetEditorWindowRect().AlignCenter(700, 700);

            // Update Info
            window.UpdateSaveDataInfo();
        }


        [TableList(IsReadOnly = true, AlwaysExpanded = true)]
        [ShowInInspector]
        [LabelText("現在ロードしているシーン内にあるSaveDataComponent")]
        private List<EdaSaveDataElementInfo> TableListWithIndexLabels = new List<EdaSaveDataElementInfo>();

        [Button("更新")]
        [PropertyOrder(-100)]
        private void UpdateSaveDataInfo()
        {
            TableListWithIndexLabels.Clear();

            // シーンの中にある すべての SaveData Component を取得する

            // Unity の SceneManager を利用して, 現在ロード済みシーンの総数を取得
            int countLoaded = SceneManager.sceneCount;

            for (int i = 0; i < countLoaded; i++)
            {
                // 現在ロード済みのシーン参照を取得する
                var loadedScene = SceneManager.GetSceneAt(i);

                // シーンを結果に代入する
                var rootGameObjects = loadedScene.GetRootGameObjects();

                // シーンのルートに配置されているGameObject の一覧
                foreach (var rootGameObject in rootGameObjects)
                {
                    _CorrectEdaSaveDataElementInfo<bool>(rootGameObject, loadedScene.name);
                    _CorrectEdaSaveDataElementInfo<int>(rootGameObject, loadedScene.name);
                    _CorrectEdaSaveDataElementInfo<float>(rootGameObject, loadedScene.name);
                    _CorrectEdaSaveDataElementInfo<string>(rootGameObject, loadedScene.name);
                    _CorrectEdaSaveDataElementInfo<UnityEngine.Vector3>(rootGameObject, loadedScene.name);
                    _CorrectEdaSaveDataElementInfo<UnityEngine.Quaternion>(rootGameObject, loadedScene.name);
                }
            }
        }

        void _CorrectEdaSaveDataElementInfo<T>(GameObject rootGameObject, string sceneName)
        {
            var scs = rootGameObject.GetComponentsInChildren<EdaSaveElementBase<T>>();
            foreach (var sc in scs)
            {
                var info = new EdaSaveDataElementInfo();
                {
                    info.Scene = sceneName;
                    info.GameObject = sc;
                    info.Key = sc.Key;
                    info.Type = typeof(T).ToString();
                    info.Memo = sc.m_memo; // Internal Access
                }
                TableListWithIndexLabels.Add(info);
            }
        }


        [PropertyOrder(-100)]
        [BoxGroup("Debbuging")]
        [ShowInInspector]
        private string savePath => Store.SavePath;

        [PropertyOrder(-100)]
        [BoxGroup("Debbuging")]
        [ShowInInspector]
        private string[] saveDataList => Store.GetAllMetaDataAbsolutePaths();


        [Serializable]
        internal class EdaSaveDataElementInfo
        {
            [ShowInInspector]
            [ReadOnly]
            [VerticalGroup("Info"), LabelWidth(40)]
            internal string Scene = System.String.Empty;

            [ShowInInspector]
            [ReadOnly]
            [VerticalGroup("Info"), LabelWidth(40)]
            internal string Key = System.String.Empty;

            [ShowInInspector]
            [ReadOnly]
            [VerticalGroup("Info"), LabelWidth(40)]
            internal string Type = "";

            [ReadOnly]
            [HideReferenceObjectPicker]
            [ShowInInspector]
            internal MonoBehaviour GameObject = null!;

            [TextArea]
            [ShowInInspector]
            [ReadOnly]
            internal string Memo = "";

            [OnInspectorInit]
            private void CreateData()
            {
                // Description = ExampleHelper.GetString();
            }
        }

    }
}
#endif