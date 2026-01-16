using UnityEngine;
using System.Collections;

public class ShootShotgun : MonoBehaviour {
    public GameObject shootee;

    public void Shoot(ItemMovement.direction dir)
    {

        GameObject go = (GameObject)Instantiate(shootee, this.transform.position, Quaternion.identity);
        go.transform.parent = this.gameObject.transform;
        if (dir == ItemMovement.direction.LEFT)
        {
            go.transform.localScale = new Vector3(go.transform.localScale.x * -1, go.transform.localScale.y, go.transform.localScale.z);
        }

    }

}