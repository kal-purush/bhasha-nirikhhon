using OVRSimpleJSON;
using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

public class ReadData : MonoBehaviour
{
    public TextAsset jsonFile;

    Dictionary<string, Dictionary<string, Dictionary<string, float>>> data;
    // Start is called before the first frame update
    void Start()
    {
        data = JsonConvert.DeserializeObject<Dictionary<string, Dictionary<string, Dictionary<string, float>>>>(jsonFile.text);
    }

    // Update is called once per frame
    void Update()
    {

    }
}