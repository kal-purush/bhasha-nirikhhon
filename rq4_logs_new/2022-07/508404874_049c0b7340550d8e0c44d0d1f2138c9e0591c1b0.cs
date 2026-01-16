using UnityEngine;

public class Collactable : MonoBehaviour, IInterractiveItem
{
    private int nameId;
    /// <summary>
    /// Hashed name string id
    /// </summary>
    /// <value></value>
    public int Name => nameId; 

    private void Awake()
    {
        nameId = Animator.StringToHash(name);
    }

    public void OnIterraction(GameObject sender)
    {
        InventoryManager inventory = sender.GetComponent<InventoryManager>();

        if (inventory == null)  
        {
            return;
        }

        inventory.AddToInventory(this);
    }
}