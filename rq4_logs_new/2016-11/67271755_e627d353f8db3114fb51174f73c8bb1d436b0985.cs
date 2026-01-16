using UnityEngine;
using UnityEngine.EventSystems;
using System.Collections;

/*Note on button click Events:
if we want to interact with game elements inside the 
environment, then we will need to
    1-create a script with the desired functions that will perform a certain action
    2-attach this script to the game object inside the environment
    3-select the button that will interact with this object.
    4-click "Add Component" -> "Event" -> "Trigger Event" and select the event type
    5-add the object you want to interact with and select the proper function from the object script
*/


public class ButtonExecute : MonoBehaviour
{
    private GameObject currentButton; // private is local to this script, public is visiable by everyscript in unity.
    public Camera CameraFacing;
    public float Timer = 2.0f;
    private float count_down;

    void Update()
    {
        RaycastHit hit;
        GameObject PushButton = null;

        Ray ray = new Ray(CameraFacing.transform.position,
                  CameraFacing.transform.rotation * Vector3.forward);

        PointerEventData data = new PointerEventData (EventSystem.current);

        if (Physics.Raycast(ray, out hit))
        {
            if (hit.transform.gameObject.tag == "Push_tag")
            {
                PushButton = hit.transform.parent.gameObject; // if the tag is detected set pushbutton to parent object
            }
        }

        if (currentButton != PushButton)
        {
            if (currentButton != null)
            { // unhighlight
                ExecuteEvents.Execute<IPointerExitHandler>(currentButton, data, ExecuteEvents.pointerExitHandler);
            }

            currentButton = PushButton;

            if (currentButton != null)
            { // highlight
                ExecuteEvents.Execute<IPointerEnterHandler>
                (currentButton, data, ExecuteEvents.pointerEnterHandler);
                count_down = Timer; // when the button is highlighed the count_down starts.
            }
        }
        //decrement the timer, and if the timer runs out then execute an action.
        if (currentButton != null)
        {
            count_down -= Time.deltaTime;
            if (count_down < 0.0f)
            {
                ExecuteEvents.Execute<IPointerClickHandler>
                (currentButton, data, ExecuteEvents.pointerClickHandler);
                count_down = Timer; // reset the count down.
            }
        }
    }//update
}// class