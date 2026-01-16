using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEditor;
using UnityEngine.UIElements;
public class PrefabTools : EditorWindow
{

    [MenuItem("PrefabTools/Open")]
    public static void ShowWindow()
    {
        // Opens the window, otherwise focuses it if it’s already open.
        var window = GetWindow<PrefabTools>();

        // Adds a title to the window.
        window.titleContent = new GUIContent("PrefabTools");

        // Sets a minimum size to the window.
        window.minSize = new Vector2(250, 50);
    }

    private void OnEnable()
    {
        #region initialization
        var root = rootVisualElement;
        root.styleSheets.Add(Resources.Load<StyleSheet>("PrefabTools_Style"));
        var quickToolVisualTree = Resources.Load<VisualTreeAsset>("PrefabTools");
        quickToolVisualTree.CloneTree(root);
    #endregion
        var toolButtons = root.Query<Button>(className: "createPrefab"); //Create Prefabs
        toolButtons.ForEach(SetupButton);

        var boxes = root.Query<Box>(className:"managerState");
        //boxes.ForEach(CheckStates);

        var refreshButton = root.Q<Button>(className: "RefreshButton");
        refreshButton.clickable.clicked += () => { boxes.ForEach(CheckStates);};
    }
    private void CheckStates(Box box)
    {
        MonoBehaviour @gameobject;
        switch (box.name)
        {
            case "AudioManager":
                @gameobject = FindObjectOfType<AudioManager>();
                break;
            case "GameManager":
                @gameobject = FindObjectOfType<GameManager>();
                break;
            default:
                @gameobject = null;
                break;
        }
        if (@gameobject != null)
        {
            box.AddToClassList("green_state");
            box.RemoveFromClassList("red_state");
        }
        else
        {
            box.AddToClassList("red_state");
            box.RemoveFromClassList("green_state");
        }
    }

    private void CheckManagers()
    {

    }

    private void SetupButton(Button button)
    {
        
        button.clickable.clicked += () => 
        {
            CreatePrefab(button.text);
        };
    }

    private void CreatePrefab(string obj)
    {
        Object prefab = AssetDatabase.LoadAssetAtPath($"Assets/Prefabs(Audio)/$h{obj}.prefab", typeof(GameObject));

        Instantiate(prefab, Vector3.zero,Quaternion.identity);
    }


    private void DebugButton(string todebug)
    {
        Debug.Log(todebug);
    }
}