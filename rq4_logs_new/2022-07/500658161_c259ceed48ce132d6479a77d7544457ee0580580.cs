using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class ItemTrigger : MonoBehaviour
{
    [SerializeField]
    private NetworkIdentity networkIdentity;

    private void OnTriggerEnter2D(Collider2D collision)
    {
        NetworkIdentity ni = collision?.gameObject?.GetComponent<NetworkIdentity>();
        if(ni.GetComponent<WhoActivatedMe>() == null && ni.IsControlling())
        {
            if(networkIdentity.TypeId == "Hp")
            {
                Slider slider = ni.getHealthBar().slider;
                if (slider.value == slider.maxValue) return;
            }
            gameObject.SetActive(false);
            networkIdentity.GetSocket().Emit("PlayerTouchItem", new JSONObject(JsonUtility.ToJson(new IDData()
            {
                id = networkIdentity.GetId(),
                enemyId = ni.GetId()
            })));
        }
    }
}