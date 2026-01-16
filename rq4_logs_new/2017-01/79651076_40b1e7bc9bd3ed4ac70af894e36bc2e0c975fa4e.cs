using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Gibbify : MonoBehaviour {

    public GameObject original;
    public WavePhysicsController controller;

    bool once;

    IEnumerator Respawn() {
        yield return new WaitForSeconds(3);

        var newCopy = Object.Instantiate(original);
        newCopy.transform.parent = transform.parent;
        var gib = newCopy.GetComponent<Gibbify>();
        gib.original = original;
        Object.Destroy(gameObject);
    }

    public void Kill() {
        if (once) {return;}
        once = true;

        controller.enabled = false;
        // controller.gameObject.GetComponent<Rigidbody>()
        var colliders = controller.gameObject.GetComponentsInChildren<MeshCollider>();
        foreach (var c in colliders) {
            var r = c.gameObject.AddComponent<Rigidbody>();
            r.gameObject.layer = LayerMask.NameToLayer("OtherPhysics");

            var smr = c.gameObject.GetComponent<SkinnedMeshRenderer>();

            var mf = c.gameObject.AddComponent<MeshFilter>();
            var mr = c.gameObject.AddComponent<MeshRenderer>();

            mf.mesh = smr.sharedMesh;
            mr.materials = smr.materials;

            r.transform.parent = transform;

            Object.Destroy(smr);
        }

        Object.Destroy(gameObject.GetComponentInChildren<CapsuleCollider>());

        Object.Destroy(gameObject.GetComponentInChildren<Stab>());

        Object.Destroy(gameObject.GetComponentInChildren<SwordyBody>().gameObject);

        StartCoroutine(Respawn());
    }
}