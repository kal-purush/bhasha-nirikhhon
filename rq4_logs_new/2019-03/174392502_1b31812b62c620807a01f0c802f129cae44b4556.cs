using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEditor;

public class ActivityCardEditor : EditorWindow
{
    //Card to be edited
    public ActivityCard activityCard;

    //Values being edited
    private string description;
    private int minimumTurn;
    private List<FeaturePrerequisiste> featurePrerequisites;
    private List<StatPrerequisite> statPrerequisites;
    private List<ChoicePrerequisite> choicePrerequisites;

    //other
    Vector2 scrollBarPosition;
    GUIStyle listItemStyle = new GUIStyle();
    Color[] colors = new Color[] { Color.white, Color.grey };
    List<int> choicePrerequisitesIndices; //list of indices for popups

    //List<string> choicePrerequisitesOptions;

    public static void Init(ActivityCard activityCard)
    {
        ActivityCardEditor window = EditorWindow.GetWindow<ActivityCardEditor>(typeof(ActivityCardEditor));

        //init editor values
        window.activityCard = activityCard;
        window.description = activityCard.description;
        window.minimumTurn = activityCard.minimumTurn;
        window.featurePrerequisites = activityCard.featurePrerequisites;
        window.statPrerequisites = activityCard.statPrerequisites;
        window.choicePrerequisites = activityCard.choicePrerequisites;

        //set up choicePrerequisites indices to point to the correct choice index in the card
        window.choicePrerequisitesIndices = new List<int>();
        foreach(var prerequisite in activityCard.choicePrerequisites)
        {
            window.choicePrerequisitesIndices.Add(prerequisite.choiceIndex);
        }
        window.Show();
    }

    //Update on change.
    private void OnFocus()
    {

        GUI.FocusControl(null);
    }

 
    private void OnGUI()
    {
        //make window scrollable
        scrollBarPosition = GUILayout.BeginScrollView(scrollBarPosition);

        //AREA FOR EDITING DESCRIPTION
        GUILayout.Label("Description", EditorStyles.boldLabel);
        GUILayout.Space(10);
        activityCard.description = EditorGUILayout.TextArea(activityCard.description, GUILayout.Height(50));
        GUILayout.Space(10);

        GUILayout.Label("Minimum turn", EditorStyles.boldLabel);
        activityCard.minimumTurn = EditorGUILayout.IntField("Minimum turn: ", activityCard.minimumTurn);

        //Area for editing choices
        GUILayout.Label("Choices", EditorStyles.boldLabel);
        if(GUILayout.Button("Add new choice"))
        {
            activityCard.choices.Add(new ActivityChoice("Choice" + activityCard.choices.Count.ToString()));
        }
        for (int i = 0; i < activityCard.choices.Count; i++)
        {
            EditorGUILayout.BeginHorizontal(listItemStyle);
            listItemStyle.normal.background = MakeTex(1, 1, colors[i % 2]);
            EditorGUILayout.BeginVertical();
            SerializedObject card = new SerializedObject(activityCard);
            EditorGUILayout.PropertyField(card.FindProperty("choices").GetArrayElementAtIndex(i), true);
            card.ApplyModifiedProperties();


            EditorGUILayout.EndVertical();
            if (GUILayout.Button("Delete"))
            {
                activityCard.choices.Remove(activityCard.choices[i]);
            }
            EditorGUILayout.EndHorizontal();
        }


        //area for editing feature prereqs
        GUILayout.Label("Feature prerequisites", EditorStyles.boldLabel);
        GUILayout.Label("Check box to require feature to be purchased. Uncheck to require feature to not have been purchased");
        GUILayout.Space(10);
        if (GUILayout.Button("Add new feature prereq"))
        {
            activityCard.featurePrerequisites.Add(new FeaturePrerequisiste());
        }
        for (int i = 0; i < activityCard.featurePrerequisites.Count; i++)
        {
            var item = activityCard.featurePrerequisites[i];
            GUILayout.BeginHorizontal();
            item.feature = EditorGUILayout.TextField("Feature: ", item.feature);
            GUILayout.Space(10);
            item.value = EditorGUILayout.Toggle("Must be purchased: ", item.value);

            if (GUILayout.Button("Delete"))
            {
                activityCard.featurePrerequisites.Remove(item);
            }
            GUILayout.EndHorizontal();
        }

        GUILayout.Space(10);

        //area for editing stat prereqs
        GUILayout.Label("Stat prerequisites", EditorStyles.boldLabel);
        GUILayout.Label("Check box to require stat to be equal or greater than target. Uncheck to require stat to be lesser than target." );
        GUILayout.Space(10);
        if (GUILayout.Button("Add new Stat prereq"))
        {
            activityCard.statPrerequisites.Add(new StatPrerequisite());
        }
        for (int i = 0; i < activityCard.statPrerequisites.Count; i++)
        {
            var item = activityCard.statPrerequisites[i];
            GUILayout.BeginHorizontal();
            item.stat = (PlayerStat)EditorGUILayout.EnumPopup("Stat: ", item.stat);
            GUILayout.Space(10);
            item.targetValue = EditorGUILayout.IntField("Target value: ", item.targetValue);
            GUILayout.Space(10);
            item.statMustBeEqualOrGreater = EditorGUILayout.Toggle("Must be equal or greater: ", item.statMustBeEqualOrGreater);
            if (GUILayout.Button("Delete"))
            {
                activityCard.statPrerequisites.Remove(item);
            }
            GUILayout.EndHorizontal();
        }

        GUILayout.Space(10);


        GUILayout.Space(10);
        GUILayout.Label("Choice prerequisites", EditorStyles.boldLabel);
        if (GUILayout.Button("Add new choice prereq"))
        {
            activityCard.choicePrerequisites.Add(new ChoicePrerequisite());
        }
        for (int i = 0; i < activityCard.choicePrerequisites.Count; i++)
        {
            var item = activityCard.choicePrerequisites[i];
            if(item != null)
            {
                GUILayout.BeginHorizontal();
                item.card = (ActivityCard)EditorGUILayout.ObjectField(item.card, typeof(ActivityCard), false);
                GUILayout.Space(10);

                if(item.card != null)
                {
                    int newChoiceIndex = EditorGUILayout.Popup("Choice", item.choiceIndex, item.card.GetChoiceTitlesArray());

                    if (newChoiceIndex != item.choiceIndex)
                    {
                        activityCard.choicePrerequisites[i].choiceIndex = newChoiceIndex;
                        EditorUtility.SetDirty(activityCard);
                    }

                }

                if (GUILayout.Button("Delete"))
                {
                    activityCard.choicePrerequisites.Remove(item);
                }

                GUILayout.EndHorizontal();
            }
        }

        //GUILayout.Button("Add");

        GUILayout.EndScrollView();
        EditorUtility.SetDirty(activityCard);

    }

    public static Texture2D MakeTex(int width, int height, Color col)
    {
        Color[] pix = new Color[width * height];

        for (int i = 0; i < pix.Length; i++)
            pix[i] = col;

        Texture2D result = new Texture2D(width, height);
        result.SetPixels(pix);
        result.Apply();

        return result;
    }
}