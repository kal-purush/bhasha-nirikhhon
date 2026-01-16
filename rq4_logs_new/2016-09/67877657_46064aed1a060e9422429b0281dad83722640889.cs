using UnityEngine;
using System.Collections;

public class MenuButton : MonoBehaviour
{
    const int TIMER_EXIT_BOUND = 3;
    int enterTimer = TIMER_EXIT_BOUND + 1;
    public MenuController menuController;
    public string sceneName;

    // Use this for initialization
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {
        if (enterTimer == TIMER_EXIT_BOUND)
        {
            menuController.OnExitButton(this);
        } else if (enterTimer == 0)
        {
            menuController.OnEnterButton(this);
        }
        enterTimer++;
    }

    void onTriggerHit(Collider collider)
    {
        if (collider.gameObject.name == "LeftControllerSphere" || collider.gameObject.name == "RightControllerSphere")
        {
            enterTimer = 0;
        }
    }

    void OnTriggerEnter(Collider collider)
    {
        onTriggerHit(collider);
    }

    void OnTriggerStay(Collider collider)
    {
        onTriggerHit(collider);
    }
}