using UnityEngine;
using UnityEditor;
using System.Collections.Generic;
using System.Reflection;
using UnityEditorInternal;
using System;
using System.Text.RegularExpressions;

namespace io.github.hatayama.HierarchyFinder
{
    /// <summary>
    /// Hierarchyでの検索を便利にするwindowです
    /// </summary>
    public class HierarchyFinderWindow : EditorWindow
    {
        private class ButtonTexts
        {
            public const string Ping = "Ping";
            public const string Paste = "Paste";
            public const string Delete = "-";
        }

        private const string PrefsKey = "HierarchySearchWindow";

        // 入力文字列を保存するリスト
        private List<string> _inputFields = new();

        // スクロール位置
        private Vector2 _scrollPosition;

        // ReorderableListを使用して並べ替え可能なリストを実装
        private ReorderableList _reorderableList;

        // JsonUtilityでシリアライズ/デシリアライズするための内部クラスを復活させる
        [Serializable]
        private class StringList
        {
            public List<string> items = new();
        }

        // 検索結果を保持するためのクラス（複製可能にする）
        public class SearchResult
        {
            public GameObject gameObject;
            public string path;

            public SearchResult(GameObject gameObject, string path)
            {
                this.gameObject = gameObject;
                this.path = path;
            }
        }

        // メニューアイテムの追加
        [MenuItem("Tools/Hierarchy Finder")]
        public static void ShowWindow()
        {
            // 既存のウィンドウを取得するか、新しいウィンドウを作成
            GetWindow<HierarchyFinderWindow>("Hierarchy Search");
        }

        private void OnGUI()
        {
            // ドラッグ＆ドロップの処理
            HandleDragAndDrop();

            EditorGUILayout.Space(2);

            // リストが空の場合のみヘルプボックスを表示
            if (_inputFields.Count == 0)
            {
                EditorGUILayout.HelpBox("D & Dでパスを自動登録。\nt: で始まる場合は「検索」で検索結果をポップアップ、「paste」で検索窓に入力します。", MessageType.Info);
                EditorGUILayout.Space(2);
            }

            // スクロールビューの開始
            _scrollPosition = EditorGUILayout.BeginScrollView(_scrollPosition);

            // ReorderableListを描画
            _reorderableList.DoLayoutList();

            // スクロールビューの終了
            EditorGUILayout.EndScrollView();
        }

        private void OnEnable()
        {
            // 保存されたリストを読み込む
            LoadSavedPaths();

            // ReorderableListの初期化
            InitializeReorderableList();
        }

        private void OnDisable()
        {
            // ウィンドウが閉じられる際にリストを保存
            SavePaths();
        }

        // 保存されたパスを読み込む
        private void LoadSavedPaths()
        {
            // EditorUserSettingsからJSON文字列を読み込む
            string json = EditorUserSettings.GetConfigValue(PrefsKey);
            if (!string.IsNullOrEmpty(json))
            {
                // JSONからリストにデシリアライズ
                StringList list = JsonUtility.FromJson<StringList>(json);
                if (list != null && list.items != null)
                {
                    _inputFields = list.items;
                }
                else
                {
                    // デシリアライズ失敗時は空リストで初期化
                    _inputFields = new List<string>();
                }
            }
            else
            {
                // 保存された値がない場合も空リストで初期化
                _inputFields = new List<string>();
            }
        }

        // パスリストを保存
        private void SavePaths()
        {
            // リストをJSON文字列にシリアライズ
            StringList list = new StringList { items = _inputFields };
            string json = JsonUtility.ToJson(list);

            // EditorUserSettingsにJSON文字列を保存
            EditorUserSettings.SetConfigValue(PrefsKey, json);
        }

        private void OnDrawElementCallback(Rect rect, int index, bool isActive, bool isFocused)
        {
            if (index < 0 || index >= _inputFields.Count) return;

            rect.y += 2;
            rect.height = EditorGUIUtility.singleLineHeight;

            string fieldValue = _inputFields[index];
            bool isTypeSearch = !string.IsNullOrEmpty(fieldValue) && fieldValue.Contains("t:");

            // 横方向のレイアウト調整（ボタンの数に応じて）
            float magnifyIconButtonWidth = 30;
            float buttonWidth = 50;
            float deleteButtonWidth = 25;
            float spacing = 4;

            float actionsWidth = buttonWidth;
            if (isTypeSearch)
            {
                actionsWidth = magnifyIconButtonWidth + buttonWidth + spacing;
            }

            float fieldWidth = rect.width - (actionsWidth + deleteButtonWidth + 8);

            // テキストフィールド
            Rect textFieldRect = new Rect(rect.x, rect.y, fieldWidth, rect.height);
            _inputFields[index] = EditorGUI.TextField(textFieldRect, _inputFields[index]);

            float currentX = rect.x + fieldWidth + spacing;

            // t:で始まる場合は2つのボタン、それ以外は1つのボタンを表示
            if (isTypeSearch)
            {
                Rect firstButtonRect = new Rect(currentX, rect.y, magnifyIconButtonWidth, rect.height);
                // 1つ目のボタン（t:の場合はSearch、通常はPing）
                Texture2D magnifyIcon = EditorGUIUtility.FindTexture("Search Icon");
                if (GUI.Button(firstButtonRect, new GUIContent(magnifyIcon)))
                {
                    // t: で始まる場合は検索してポップアップ表示
                    SearchObjectsAndShowPopup(fieldValue, firstButtonRect);
                    GUIUtility.ExitGUI(); // ポップアップ表示後にGUIを再描画
                }

                currentX += magnifyIconButtonWidth + spacing;
                Rect pasteButtonRect = new Rect(currentX, rect.y, buttonWidth, rect.height);
                // t:で始まる場合のみ2つ目のボタン（Paste）を表示
                if (GUI.Button(pasteButtonRect, ButtonTexts.Paste))
                {
                    SetHierarchySearchFilter(fieldValue);
                }
            }
            else
            {
                Rect firstButtonRect = new Rect(currentX, rect.y, buttonWidth, rect.height);
                if (GUI.Button(firstButtonRect, ButtonTexts.Ping))
                {
                    // 通常のパスからGameObjectを検索してPing
                    PingObject(index, firstButtonRect);
                }
            }

            // 削除ボタンを右端に追加
            Rect deleteButtonRect = new Rect(rect.x + rect.width - deleteButtonWidth, rect.y, deleteButtonWidth, rect.height);
            GUIStyle deleteButtonStyle = new GUIStyle(EditorStyles.miniButton);
            deleteButtonStyle.alignment = TextAnchor.MiddleCenter;

            if (GUI.Button(deleteButtonRect, ButtonTexts.Delete, deleteButtonStyle))
            {
                _inputFields.RemoveAt(index);
                SavePaths(); // 保存処理を呼び出す
                Repaint();
            }
        }

        // ReorderableListの初期化
        private void InitializeReorderableList()
        {
            _reorderableList = new ReorderableList(_inputFields, typeof(string), true, false, true, false);
            _reorderableList.drawElementCallback = OnDrawElementCallback;
            _reorderableList.headerHeight = 0;
            _reorderableList.elementHeight = EditorGUIUtility.singleLineHeight + 6;

            // 新しい要素の追加処理
            _reorderableList.onAddCallback = (ReorderableList list) =>
            {
                _inputFields.Add("");
                SavePaths(); // 保存処理を呼び出す
            };

            _reorderableList.onReorderCallback = (ReorderableList list) =>
            {
                SavePaths(); // 保存処理を呼び出す
            };
        }

        // 通常のパスからGameObjectを検索してPing
        private void PingObject(int index, Rect buttonRect = default)
        {
            if (index < 0 || index >= _inputFields.Count) return;

            string fieldValue = _inputFields[index];
            GameObject targetObject = GameObject.Find(fieldValue);
            if (targetObject != null)
            {
                // GameObjectをPing（ハイライト表示）
                EditorGUIUtility.PingObject(targetObject);

                // GameObjectを選択状態にする（Inspectorに表示）
                Selection.activeGameObject = targetObject;
                return;
            }

            // GameObjectが見つからない場合はポップアップを表示
            SearchResultPopupWindow.Show(buttonRect, new List<SearchResult>(), null, "");

        }

        // t:で始まる文字列で検索を実行しポップアップを表示
        private void SearchObjectsAndShowPopup(string searchQuery, Rect buttonRect)
        {
            // このメソッド内でのみ使用する検索結果リスト
            List<SearchResult> searchResults = new List<SearchResult>();
            SearchObjects(searchQuery, searchResults);

            // クリックしたボタンの下にポップアップを表示（ソースインデックスも渡す）
            SearchResultPopupWindow.Show(buttonRect, searchResults, (selectedObject) =>
            {
                // オブジェクトをPingしてハイライト表示
                EditorGUIUtility.PingObject(selectedObject);

                // オブジェクトを選択状態にする
                Selection.activeGameObject = selectedObject;
            }, searchQuery);
        }

        private (string, string) GetTypeAndName(string searchQuery)
        {
            // パターン1: [任意の空白]t:型名 [オプショナルな名前]
            string pattern1 = @"^\s*t:\s*(?<type>[^\s]+)(?:\s+(?<name>[^\s]+))?\s*$";

            // パターン2: [任意の空白]名前 [任意の空白]t:型名
            string pattern2 = @"^\s*(?<name>[^\s]+)\s+t:\s*(?<type>[^\s]+)\s*$";

            // 最終的な統合パターン
            string combinedPattern = pattern1 + "|" + pattern2;
            Match match = Regex.Match(searchQuery, combinedPattern);

            if (match.Success)
            {
                // グループ名で値を抽出
                var type = match.Groups["type"].Value;
                var goName = match.Groups["name"].Value;
                return (type, goName);
            }

            Debug.LogError("パターンに一致する文字列が見つかりませんでした。");
            return (null, null);
        }

        public bool ContainsIgnoreCase(string source, string searchTerm)
        {
            if (source == null || searchTerm == null) return false;

            return source.IndexOf(searchTerm, StringComparison.OrdinalIgnoreCase) >= 0;
        }

        private void SearchObjects(string searchQuery, List<SearchResult> results)
        {
            // 検索条件の解析（例: "t:Light" -> "Light"）
            (string typeToFind, string goName) = GetTypeAndName(searchQuery);
            GameObject[] gameObjects = FindObjectsByType<GameObject>(FindObjectsInactive.Include, FindObjectsSortMode.None);
            bool isNameMissing = string.IsNullOrEmpty(goName);

            // 特殊ケース。t:GameObjectだった場合
            if (typeToFind.Equals("GameObject", StringComparison.OrdinalIgnoreCase))
            {
                foreach (var obj in gameObjects)
                {
                    if (isNameMissing)
                    {
                        results.Add(new SearchResult(obj, GetGameObjectPath(obj)));
                        continue;
                    }

                    if (ContainsIgnoreCase(obj.name, goName))
                    {
                        results.Add(new SearchResult(obj, GetGameObjectPath(obj)));
                    }
                }

                return;
            }

            foreach (GameObject obj in gameObjects)
            {
                bool isMatch = false;
                // コンポーネントをチェック
                Component[] components = obj.GetComponents<Component>();
                foreach (Component comp in components)
                {
                    if (comp == null) continue;

                    Type componentType = comp.GetType();

                    // コンポーネントタイプとその全ての親クラスをチェック
                    while (componentType != null && componentType != typeof(object))
                    {
                        string typeName = componentType.Name;

                        // 型名を検索 (例: "Image", "Transform" など)
                        if (typeName.Equals(typeToFind, StringComparison.OrdinalIgnoreCase) ||
                            // UnityEngine.UI.Image などの場合は末尾の型名のみでマッチ
                            typeName.EndsWith("." + typeToFind, StringComparison.OrdinalIgnoreCase))
                        {
                            isMatch = true;
                            break;
                        }

                        // 親クラスに移動
                        componentType = componentType.BaseType;
                    }

                    if (isMatch) break;
                }

                // マッチした場合は結果リストに追加
                if (isMatch)
                {
                    if (isNameMissing)
                    {
                        results.Add(new SearchResult(obj, GetGameObjectPath(obj)));
                        continue;
                    }

                    if (ContainsIgnoreCase(obj.name, goName))
                    {
                        results.Add(new SearchResult(obj, GetGameObjectPath(obj)));
                    }
                }
            }
        }

        private void HandleDragAndDrop()
        {
            Event evt = Event.current;
            if (evt.type != EventType.DragUpdated && evt.type != EventType.DragPerform)
            {
                return;
            }

            // ドラッグされているオブジェクトを受け入れられるようにカーソルを変更
            DragAndDrop.visualMode = DragAndDropVisualMode.Copy;

            if (evt.type == EventType.DragPerform)
            {
                // ドラッグ＆ドロップ操作を完了
                DragAndDrop.AcceptDrag();

                // ドロップされたオブジェクトの処理
                foreach (UnityEngine.Object draggedObject in DragAndDrop.objectReferences)
                {
                    GameObject go = draggedObject as GameObject;
                    if (go != null)
                    {
                        // GameObjectのHierarchyパスを取得
                        string hierarchyPath = GetGameObjectPath(go);

                        // 新しいフィールドを追加
                        _inputFields.Add(hierarchyPath);
                        SavePaths(); // 変更を保存
                        Repaint();
                    }
                }
            }

            evt.Use(); // イベントを消費して他のコントロールに影響しないようにする
        }

        // GameObjectのHierarchyパスを取得
        private string GetGameObjectPath(GameObject obj)
        {
            string path = obj.name;
            Transform parent = obj.transform.parent;

            while (parent != null)
            {
                path = parent.name + "/" + path;
                parent = parent.parent;
            }

            return path;
        }


        // Hierarchyの検索窓に検索文字列を設定するメソッド
        private void SetHierarchySearchFilter(string searchString)
        {
            // EditorApplication.ExecuteMenuItem("Window/General/Hierarchy"); // Hierarchyウィンドウを強制的に開かないようにコメントアウトしておくか？

            // SearchableEditorWindowタイプ（SceneHierarchyWindowの親クラス）を取得
            System.Type searchableEditorWindowType = typeof(EditorWindow).Assembly.GetType("UnityEditor.SearchableEditorWindow");
            if (searchableEditorWindowType == null)
            {
                Debug.LogError("SearchableEditorWindowタイプが見つかりませんでした");
                return;
            }

            // SetSearchFilter メソッドをリフレクションで取得
            var setSearchFilterMethod = searchableEditorWindowType.GetMethod("SetSearchFilter",
                BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);

            if (setSearchFilterMethod == null)
            {
                Debug.LogError("検索メソッドが見つかりませんでした");
                return;
            }

            // 検索メソッドを実行
            setSearchFilterMethod.Invoke(focusedWindow,
                new object[] { searchString, SearchableEditorWindow.SearchMode.All, true, false });

            // ウィンドウを再描画
            focusedWindow.Repaint();
        }
    }
}