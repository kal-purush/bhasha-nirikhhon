using UnityEngine;
using System.Collections;

public class CameraCtrl : MonoBehaviour {

	public GameObject player;
    public bool quoter;

	// Use this for initialization
	void Start () {

	}

	// Update is called once per frame
	void Update () {
        //transform.position = new Vector3(player.transform.position.x+4f, player.transform.position.y+2.8f, player.transform.position.z - 1.5f);

        ////指定した地点から、指定方向の角度を取得する
        //var forward = Quaternion.LookRotation(new Vector3(-1f, -1f, 1f));

        //transform.rotation = forward;

        //近め
        //transform.position = new Vector3(player.transform.position.x + 1.7f, player.transform.position.y + 2.6f, player.transform.position.z - 1.7f);
        //transform.rotation = Quaternion.Euler(new Vector3(25, -56, -18));

        //中くらい
        //transform.position = new Vector3(player.transform.position.x + 2.9f, player.transform.position.y + 3.3f, player.transform.position.z - 2.4f);
        //transform.rotation = Quaternion.Euler(new Vector3(25, -56, -18));

        if (quoter)
        {
            //遠目
            transform.position = new Vector3(player.transform.position.x + 2.8f, player.transform.position.y + 4.9f, player.transform.position.z - 3.2f);
            transform.rotation = Quaternion.Euler(new Vector3(40, 315, 345));
        }
        else
        {
            //真後ろ
            transform.position = new Vector3(player.transform.position.x, player.transform.position.y + 1.3f, player.transform.position.z);



            //Vector3 next = new Vector3(transform.rotation.x, transform.rotation.y, transform.rotation.z);
            //Vector3 diff = transform.rotation.eulerAngles - next;

            //transform.rotation = Quaternion.Euler(transform.rotation.eulerAngles - diff);

            transform.rotation = new Quaternion(player.transform.rotation.x,
                                                player.transform.rotation.y,
                                                player.transform.rotation.z,
                                                player.transform.rotation.w);
        }
    }
}