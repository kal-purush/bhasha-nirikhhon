using UnityEngine;
using UnityEngine.EventSystems;

public class CloseOnClickOutside : MonoBehaviour
{
    public GameObject profilePanel;

    void Update()
    {
        if (Input.GetMouseButtonDown(0))
        {
            if (!IsPointerOverUIElement() && profilePanel.activeSelf)
            {
                profilePanel.SetActive(false);
            }
        }
    }

    private bool IsPointerOverUIElement()
    {
        PointerEventData eventData = new PointerEventData(EventSystem.current)
        {
            position = Input.mousePosition
        };

        var results = new System.Collections.Generic.List<RaycastResult>();
        EventSystem.current.RaycastAll(eventData, results);

        foreach (var result in results)
        {
            if (result.gameObject == profilePanel || result.gameObject.transform.IsChildOf(profilePanel.transform))
                return true;
        }

        return false;
    }
}