using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DeliverySpot : MonoBehaviour
{
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public bool deliverProduct(ProductInstance product)
    {
        // Comprobar también si está en la lista de pedidos 
        if(product.getProductType() == ProductType.FINAL)
        {
            // Avisar al manager que lleve eso
            Destroy(product.gameObject);
            return true;
        }
        return false;
    }
}