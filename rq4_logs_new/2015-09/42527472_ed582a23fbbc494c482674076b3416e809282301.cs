/**
@author Darrick Hilburn

This script writes scene data to an external text file through
commands in an editor window.
*/

using UnityEngine;
using UnityEditor;
using System.Collections;
using System.IO;
using System.Text;

public class ScriptExportLevelData : EditorWindow
{
    string levelName = "";
    string creationDate = "";
    string creatorName = "";

    string month = "";
    string day = "";
    string year = "";

    // Initialize the window area
    public static void Init()
    {
        ScriptExportLevelData window = (ScriptExportLevelData)EditorWindow.GetWindow(typeof(ScriptExportLevelData));
        window.position = new Rect(500, 500, 310, 80);
        window.maxSize = new Vector2(310, 80);
        window.minSize = window.maxSize;
        window.Show();
    }

    void OnGUI()
    {
        GUILayout.BeginVertical();
            GUILayout.BeginHorizontal();
                // Set level name
                GUILayout.Label("Level name: ");
                levelName = GUILayout.TextField(levelName);
            GUILayout.EndHorizontal();
            GUILayout.BeginHorizontal();
                // Set creation date
                GUILayout.Label("Creation date: ");
                month = GUILayout.TextField(month);
                GUILayout.Label("/");
                day = GUILayout.TextField(day);
                GUILayout.Label("/");
                year = GUILayout.TextField(year);
                creationDate = string.Format("{0}/{1}/{2}",month,day,year);
            GUILayout.EndHorizontal();
            GUILayout.BeginHorizontal();
                // Set creator name
                GUILayout.Label("Creator name: ");
                creatorName = GUILayout.TextField(creatorName);
            GUILayout.EndHorizontal();
            GUILayout.BeginHorizontal();
                // Call ExportData to export level data to an external file
                if(GUILayout.Button("Export"))
                {
                    ExportData();
                }
            GUILayout.EndHorizontal();
        GUILayout.EndVertical();
    }

    /**
    ExportData sends all waypoint data and other level data to an external text file.
    */
    void ExportData()
    {
        StringBuilder data;
        StreamWriter writer = new StreamWriter(levelName + ".txt.", false);
        ScriptMovements[] moves = GameObject.Find("Player").GetComponent<ScriptEngine>().movements;
        ScriptEffects[] effects = GameObject.Find("Player").GetComponent<ScriptEngine>().effects;
        ScriptFacings[] facings = GameObject.Find("Player").GetComponent<ScriptEngine>().facings;

        // Write level data
        writer.WriteLine(levelName);
        writer.WriteLine(creationDate);
        writer.WriteLine(creatorName);
        // Write all movements
        foreach(ScriptMovements move in moves)
        {
            data = new StringBuilder();
            switch(move.moveType)
            {
                case(MovementTypes.WAIT):
                    data.Append("M_wait ");
                    // WAIT POSITION
                    data.Append(move.movementTime);
                    break;
                case(MovementTypes.MOVE):
                    data.Append("M_move ");
                    data.Append(move.movementTime + " ");
                    // START POSITION
                    data.Append(string.Format("{0},{1},{2}", move.endWaypoint.transform.position.x, move.endWaypoint.transform.position.y, move.endWaypoint.transform.position.z));
                    break;
                case(MovementTypes.BEZIER):
                    data.Append("M_bezier ");
                    data.Append(move.movementTime + " ");
                    // START POSITION
                    data.Append(string.Format("{0},{1},{2} ", move.endWaypoint.transform.position.x, move.endWaypoint.transform.position.y, move.endWaypoint.transform.position.z));
                    data.Append(string.Format("{0},{1},{2}", move.curveWaypoint.transform.position.x, move.curveWaypoint.transform.position.y, move.curveWaypoint.transform.position.z));
                    break;
            }
            writer.WriteLine(data.ToString());
        }
        // Write all effects
        foreach (ScriptEffects effect in effects)
        {
            data = new StringBuilder();
            switch (effect.effectType)
            {
                case (EffectTypes.FADE):
                    data.Append("E_fade ");
                    data.Append(effect.effectTime + " ");
                    data.Append(effect.fadeInTime + " ");
                    data.Append(effect.fadeOutTime + " ");
                    break;
                case (EffectTypes.SHAKE):
                    data.Append("E_shake ");
                    data.Append(effect.effectTime + " ");
                    data.Append(effect.magnitude);
                    break;
                case (EffectTypes.SPLATTER):
                    data.Append("E_splatter");
                    data.Append(effect.effectTime + " ");
                    data.Append(effect.fadeInTime + " ");
                    data.Append(effect.fadeOutTime + " ");
                    data.Append(effect.imageScale);
                    break;
                case (EffectTypes.WAIT):
                    data.Append("E_wait");
                    data.Append(effect.effectTime);
                    break;
            }
            writer.WriteLine(data.ToString());
        }
        // Write all facings
        foreach (ScriptFacings facing in facings)
        {
            data = new StringBuilder();
            switch(facing.facingType)
            {
                case (FacingTypes.FREELOOK):
                    break;
                case (FacingTypes.LOOKAT):
                    data.Append("F_lookat ");
                    data.Append(facing.facingTime + " ");
                    data.Append(string.Format("{0},{1},{2} ",facing.targets[0].transform.position.x, facing.targets[0].transform.position.y, facing.targets[0].transform.position.z));
                    data.Append(facing.rotationSpeed[0] + " ");
                    data.Append(facing.lockTimes[0] + " ");
                    break;
                case (FacingTypes.LOOKCHAIN):
                    data.Append("F_lookchain ");
                    data.Append(facing.facingTime + " ");
                    for(int i = 0; i < facing.targets.Length; i++)
                    {
                        data.Append(string.Format("{0},{1},{2} ", facing.targets[i].transform.position.x, facing.targets[i].transform.position.y, facing.targets[i].transform.position.z));
                        data.Append(facing.rotationSpeed[i] + " ");
                        data.Append(facing.lockTimes[i] + " ");
                    }
                    break;
                case (FacingTypes.WAIT):
                    data.Append("F_wait ");
                    data.Append(facing.facingTime);
                    break;
            }
            writer.WriteLine(data.ToString());
        }
        writer.Close();
    }
}