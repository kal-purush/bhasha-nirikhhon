using System.IO;
using UnityEngine;

namespace Assets.Scripts
{
    public class CollectItem : MonoBehaviour
    {
        private Inventory _inventory;

        void Start()
        {
            _inventory = GetComponent<Inventory>();
        }

        // Update is called once per frame
        void Update ()
        {
            RaycastHit hitInfo;
            var ray = new Ray(transform.position, transform.forward);
            if (Physics.Raycast(ray, out hitInfo, 5f))
            {
                if (hitInfo.collider.tag == "Item")
                {
                    var item = hitInfo.collider.gameObject;
                    if(Input.GetKeyDown(KeyCode.E))
                    {
                        _inventory.AddItem(item.GetComponent<ShopItem>().ShopItemData);
                        Destroy(gameObject);
                    }
                }
            }
        }
    }
}