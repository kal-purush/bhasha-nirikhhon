using UnityEngine;
using System.Collections;

public class AbleBuilding : MonoBehaviour
{
    private void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.tag == "FriendlyforcesBuild")
        {
            other.gameObject.GetComponent<FriendlyforcesBuild>().AbleBuild = true;
        }
    }

    private void OnTriggerExit(Collider other)
    {
        if (other.gameObject.tag == "FriendlyforcesBuild")
        {
            other.gameObject.GetComponent<FriendlyforcesBuild>().AbleBuild = false;
        }
    }

}