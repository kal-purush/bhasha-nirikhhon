using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System.Linq;

public class TacticsManager : MonoBehaviour
{

    // todo: tactics from json
    public TextAsset tacticsJson;
    public SkillDatabase skillData;

    private EventController controller;

    void Start()
    {
        controller = GameObject.Find("EventController").GetComponent<EventController>();
    }

    // all tactics will be "self" or "instant", similar to allySingle
    public IEnumerator UseTactic(Character user, string tactic)
    {
        yield return null;
    }

    private bool ValidConditions(Character user, string tacticName)
    {
        return true;
    }

    public Skill GetTacticByName(string name)
    {
        return skillData.skills.Find(x => x.name == name);
    }

}