using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEditor;

public class Scr_QuestEditor : EditorWindow
{
    private Scr_QuestData inventoryItemList;
    private int viewIndex = 1;
    private int eventNum = 0;

    [MenuItem("Window/Quest Editor")]
    static void Init()
    {
        EditorWindow.GetWindow(typeof(Scr_QuestEditor));
    }

    private void OnEnable()
    {
        if (EditorPrefs.HasKey("objectPath"))
        {
            string ObjectPath = "Assets/Resources/Data/Data_QuestData.asset";
            inventoryItemList = AssetDatabase.LoadAssetAtPath(ObjectPath, typeof(Scr_QuestData)) as Scr_QuestData;
        }

        if (inventoryItemList == null)
        {
            viewIndex = 1;

            Scr_QuestData asset = ScriptableObject.CreateInstance<Scr_QuestData>();
            AssetDatabase.CreateAsset(asset, "Assets/Resources/Data/Data_QuestData.asset");
            AssetDatabase.SaveAssets();

            inventoryItemList = asset;

            if (inventoryItemList)
            {
                inventoryItemList.QuestList = new List<Scr_QuestInfo>();
                string relPath = AssetDatabase.GetAssetPath(inventoryItemList);
                EditorPrefs.SetString("objectPath", relPath);
            }
        }
    }
    private void OnGUI()
    {
        GUILayout.Label("Quest Editor", EditorStyles.boldLabel);
        GUILayout.Space(10);

        if (inventoryItemList != null)
        {
            PrintTopMenu();
        }

        else
        {
            GUILayout.Space(10);
            GUILayout.Label("Can't load quest list.");
        }

        if (GUI.changed)
        {
            EditorUtility.SetDirty(inventoryItemList);
        }
    }

    void PrintTopMenu()
    {
        GUILayout.BeginHorizontal();
        GUILayout.Space(10);

        if (GUILayout.Button("<- Prev", GUILayout.ExpandWidth(false)))
        {
            if (viewIndex > 1)
                viewIndex -= 1;
        }

        GUILayout.Space(5);

        if (GUILayout.Button("Next ->", GUILayout.ExpandWidth(false)))
        {
            if (viewIndex < inventoryItemList.QuestList.Count)
                viewIndex += 1;
        }

        GUILayout.Space(60);

        if (GUILayout.Button("+ Add Quest", GUILayout.ExpandWidth(false)))
        {
            AddQuest();
        }

        GUILayout.Space(5);

        if (GUILayout.Button("- Delete Quest", GUILayout.ExpandWidth(false)))
        {
            DeleteQuest(viewIndex - 1);
        }

        GUILayout.EndHorizontal();

        if (inventoryItemList.QuestList.Count > 0)
        {
            QuestListMenu();
        }

        else
        {
            GUILayout.Space(10);
            GUILayout.Label("This Quest List is Empty.");
        }

        GUILayout.BeginHorizontal();

        if (GUILayout.Button("+ Add Event", GUILayout.ExpandWidth(false)))
        {
            AddEvent();
        }

        GUILayout.Space(5);

        if (GUILayout.Button("- Remove Event", GUILayout.ExpandWidth(false)))
        {
            RemoveEvent();
        }

        GUILayout.EndHorizontal();
    }

    void AddQuest()
    {
        Scr_QuestInfo newCraft = new Scr_QuestInfo();
        newCraft.name = "New Quest";
        inventoryItemList.QuestList.Add(newCraft);
        viewIndex = inventoryItemList.QuestList.Count;
    }

    void DeleteQuest(int index)
    {
        inventoryItemList.QuestList.RemoveAt(index);
    }

    void AddEvent()
    {
        Scr_EventInfo newEvent = new Scr_EventInfo();
        inventoryItemList.QuestList[viewIndex - 1].EventList.Add(newEvent);
        eventNum += 1;
    }

    void RemoveEvent()
    {
        inventoryItemList.QuestList[viewIndex - 1].EventList.RemoveAt(eventNum - 1);
        eventNum -= 1;
    }

    void QuestListMenu()
    {
        GUILayout.Space(10);
        GUILayout.BeginHorizontal();
        viewIndex = Mathf.Clamp(EditorGUILayout.IntField("Current Quest", viewIndex, GUILayout.ExpandWidth(false)), 1, inventoryItemList.QuestList.Count);
        EditorGUILayout.LabelField("of " + inventoryItemList.QuestList.Count.ToString() + " Quest", "", GUILayout.ExpandWidth(false));
        GUILayout.EndHorizontal();

        string[] _choices = new string[inventoryItemList.QuestList.Count];
        for (int i = 0; i < inventoryItemList.QuestList.Count; i++)
        {
            _choices[i] = inventoryItemList.QuestList[i].name;
        }

        int _choicesIndex = viewIndex - 1;
        viewIndex = EditorGUILayout.Popup(_choicesIndex, _choices) + 1;

        GUILayout.Space(10);
        inventoryItemList.QuestList[viewIndex - 1].name = EditorGUILayout.TextField("Name", inventoryItemList.QuestList[viewIndex - 1].name as string);
        inventoryItemList.QuestList[viewIndex - 1].description = EditorGUILayout.TextField("Description", inventoryItemList.QuestList[viewIndex - 1].description as string);

        GUILayout.Space(10);
        GUILayout.Label("Events");

        for (int i = 0; i < inventoryItemList.QuestList[viewIndex - 1].EventList.Count; i++)
        {
            inventoryItemList.QuestList[viewIndex - 1].EventList[i].name = EditorGUILayout.TextField("Name", inventoryItemList.QuestList[viewIndex - 1].EventList[i].name as string);
            inventoryItemList.QuestList[viewIndex - 1].EventList[i].description = EditorGUILayout.TextField("Description", inventoryItemList.QuestList[viewIndex - 1].EventList[i].description as string);
            GUILayout.Space(5);
            inventoryItemList.QuestList[viewIndex - 1].EventList[i].from = EditorGUILayout.IntField("From", inventoryItemList.QuestList[viewIndex - 1].EventList[i].from);
            inventoryItemList.QuestList[viewIndex - 1].EventList[i].to = EditorGUILayout.IntField("From", inventoryItemList.QuestList[viewIndex - 1].EventList[i].to);
            GUILayout.Space(10);
        }
    }
}