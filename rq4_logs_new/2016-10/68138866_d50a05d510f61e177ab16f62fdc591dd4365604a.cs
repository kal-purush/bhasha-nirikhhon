using UnityEngine;
using System.Collections;

public class click6 : MonoBehaviour {

    public Main2 main;
    public void OnMouseDown()
    {
        Debug.Log("clcked5");
        main.testCorrect(5);
    }

    void OnSelect()
    {

        ;
        // If the sphere has no Rigidbody component, add one to enable physics.
        if (!this.GetComponent<Rigidbody>())
        {
            //  var rigidbody = this.gameObject.AddComponent<Rigidbody>();
            // rigidbody.collisionDetectionMode = CollisionDetectionMode.Continuous;
        }
        OnMouseDown();
    }
}