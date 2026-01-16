using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System.Linq;

public class ActionManager : MonoBehaviour
{
    protected EventController controller;    

    protected void initController()
    {
        if (controller == null) controller = GetComponent<EventController>();
    }

    public IEnumerator useAction(Character user, Character target, Action action)
    {
        controller.resetTargetting();
        controller.actionMenu.HideButtons();
        controller.clearDescription();

        //todo: Make animation stuff a function
        controller.DisplaySkillDialogue(user, action.Alias, 1.0f);
        UnityEngine.Debug.Log(user.name);
        UnityEngine.Debug.Log(target.characterName);
        UnityEngine.Debug.Log(target.health);


        yield return action.performAction(user, new Character[] { target });
        if (controller.checkLife())
        {
            controller.nextTurn();

        }
    }


}