using UnityEngine;

public class ApplySlot : MonoBehaviour, IInterractiveItem
{
    public string itemName;

    [SerializeField] private int itemId;

    private void Start()
    {
        itemId = Animator.StringToHash(itemName);
        itemName = null;
    }

    public void OnIterraction(GameObject sender)
    {
        InventoryManager inventory = sender.GetComponent<InventoryManager>();
        Appliable needed = inventory?.GetFromInventory(itemId)?.GetComponent<Appliable>();

        if (needed == null)
        {
            return;
        }

        needed.Apply();
    }
}