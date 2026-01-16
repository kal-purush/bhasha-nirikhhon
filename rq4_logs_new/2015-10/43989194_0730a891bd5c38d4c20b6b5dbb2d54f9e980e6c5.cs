using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class splash : MonoBehaviour {

	public GameObject pequenoSplash;

	private List<GameObject> noCollider = new List<GameObject>();
	private Dictionary<int,ultimoSplash> ultimo = new Dictionary<int, ultimoSplash>();


	private struct ultimoSplash
	{
		public float tempo;
		public Vector3 pos;
	}
	// Use this for initialization
	void Start () {

	}
	
	// Update is called once per frame
	void Update () {
		if(noCollider.Count>0)
		{
			int i = -1;
			foreach( GameObject G in noCollider )
			{

				if(ultimo.ContainsKey(noCollider.IndexOf(G)))
				{
					ultimoSplash temp = ultimo[noCollider.IndexOf(G)];
					i = noCollider.IndexOf(G);
					if(G)
					{
						temp.pos = G.transform.position;

						if(temp.pos!=ultimo[i].pos && ultimo[i].tempo<Time.time-0.1f)
						{

							temp.pos = G.transform.position;
							temp.tempo = Time.time;
							Destroy(
							Instantiate(pequenoSplash, ultimo[i].pos+0.3f*Vector3.up,Quaternion.identity)
								,1);
						}

						ultimo[i] = temp;
					}else
						ultimo.Remove(i);
				}
			}
		}
	}

	void OnTriggerEnter(Collider col)
	{
		if(col.tag=="Criature" || col.tag=="Player")
		{
			noCollider.Add(col.gameObject);
			ultimo[
				noCollider.IndexOf(col.gameObject)] = 
				new ultimoSplash(){tempo = Time.time,pos = col.transform.position}
				;
		}
	}

	void OnTriggerExit(Collider col)
	{
		if(col.tag=="Criature" || col.tag=="Player")
		{
			ultimo.Remove(noCollider.IndexOf(col.gameObject));
			noCollider.Remove(col.gameObject);
		}
	}
}