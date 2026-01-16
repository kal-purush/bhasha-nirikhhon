using System.Collections.Generic;
using Comfort.Common;
using DebugPlus.Config;
using DebugPlus.Utils;
using UnityEngine;
using OCB = DebugPlus.Utils.OverlayContentBuilder;

namespace DebugPlus.Components;

public class DisplayJson : MonoBehaviour
{
    private List<GameObject> spheres;
    private void Awake()
    {
        spheres = new List<GameObject>();
        string filepath = "/BepInEx/plugins/file.json";
        LocationsJson locationJson = JsonHelper.ParseJsonFromFile<LocationsJson>(filepath);
        Plugin.Log.LogInfo(JsonHelper.SerializeJson(locationJson));
        string mapName = Singleton<EFT.GameWorld>.Instance.MainPlayer.Location.ToLower();
        Plugin.Log.LogInfo("Loading: " + mapName);

        foreach (var location in locationJson.locations[mapName])
        {
            var sphere = GameObject.CreatePrimitive(PrimitiveType.Sphere);
            sphere.GetComponent<Collider>().enabled = location.physics;
            sphere.transform.position = location.coordinates;
            sphere.transform.localScale = location.objectScale;
            sphere.GetComponent<Renderer>().material.color = location.objectColor;
            Plugin.Log.LogInfo(location.text);

            sphere.GetOrAddComponent<OverlayProvider>().SetOverlayContent(GetJsonText(location), Enable);

            spheres.Add(sphere);
        }
    }

    private static bool Enable()
    {
        return DebugPlusConfig.ShowJsonOverlay.Value;
    }

    private static string GetJsonText(JsonLocationEntry entry)
    {
        OCB.Clear();

        OCB.AppendLabeledValue(entry.label, entry.text, entry.labelColor, entry.textColor);

        return OCB.ToString();
    }
}