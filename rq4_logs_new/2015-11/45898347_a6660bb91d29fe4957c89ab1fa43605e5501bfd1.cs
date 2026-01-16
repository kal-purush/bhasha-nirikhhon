using UnityEngine;
using System.Collections.Generic;
using System.IO;
using System.Collections;

public class FactinGenerator : MonoBehaviour
{

    // すべてのMicroTubeに共通の設定になると思うので，
    // colorとwidthは生成するクラスが持ってる
    // この値はインスペクタからいじれる(SerializeField)
    [SerializeField] Color color;
    [SerializeField] float width;

	float r1 = 621.09f, r2 = 690.10f, r, theta, phi, layer1, layer2;
	List<MicroTube> actin = new List<MicroTube> ();

	void Start ()
	{
		layer1 = (r2 - r1) / 3f + r1;
		layer2 = 2f * (r2 - r1) / 3f + r1;
		//for (j=0; j<19; j++) {
		var j = 1;
		for (var i = 0; i < 36; i++) {
			r = r1;
			phi = i / 18f * 3.14f;
			theta = j / 18f * 3.14f;
			float x = r * Mathf.Sin (theta) * Mathf.Cos (phi);
			float y = r * Mathf.Sin (theta) * Mathf.Sin (phi);
			float z = r * Mathf.Cos (theta);
			var start = new Vector3(x, y, z);

			r = layer1;
			phi = i / 18f * 3.14f;
			theta = j / 18f * 3.14f;
			x = r * Mathf.Sin (theta) * Mathf.Cos (phi);
			y = r * Mathf.Sin (theta) * Mathf.Sin (phi);
			z = r * Mathf.Cos (theta);
			var end = new Vector3(x, y, z);

			var microTube = MicroTube.Instantiate(start, end);
            microTube.ApplyColor(color);
            microTube.ApplyWidth(width);
            microTube.transform.SetParent(transform, false);
            actin.Add(microTube);

			/*
				phi = i  / 18f * 3.14f;
				theta = j+1  / 18f * 3.14f;
				x = r * Mathf.Sin (theta) * Mathf.Cos (phi);
				y = r * Mathf.Sin (theta) * Mathf.Sin (phi);
				z = r * Mathf.Cos (theta);
				end.setPoint (x, y, z);
				actin.Add (start);
				actin.Add (end);

				phi = i  / 18f * 3.14f;
				theta = j-1  / 18f * 3.14f;
				x = r * Mathf.Sin (theta) * Mathf.Cos (phi);
				y = r * Mathf.Sin (theta) * Mathf.Sin (phi);
				z = r * Mathf.Cos (theta);
				end.setPoint (x, y, z);
				actin.Add (start);
				actin.Add (end);

				phi = i-1  / 18f * 3.14f;
				theta = j  / 18f * 3.14f;
				x = r * Mathf.Sin (theta) * Mathf.Cos (phi);
				y = r * Mathf.Sin (theta) * Mathf.Sin (phi);
				z = r * Mathf.Cos (theta);
				end.setPoint (x, y, z);
				actin.Add (start);
				actin.Add (end);;

				phi = i-1  / 18f * 3.14f;
				theta = j+1  / 18f * 3.14f;
				x = r * Mathf.Sin (theta) * Mathf.Cos (phi);
				y = r * Mathf.Sin (theta) * Mathf.Sin (phi);
				z = r * Mathf.Cos (theta);
				end.setPoint (x, y, z);
				actin.Add (start);
				actin.Add (end);

				phi = i-1  / 18f * 3.14f;
				theta = j-1  / 18f * 3.14f;
				x = r * Mathf.Sin (theta) * Mathf.Cos (phi);
				y = r * Mathf.Sin (theta) * Mathf.Sin (phi);
				z = r * Mathf.Cos (theta);
				end.setPoint (x, y, z);
				actin.Add (start);
				actin.Add (end);

				phi = i+1  / 18f * 3.14f;
				theta = j  / 18f * 3.14f;
				x = r * Mathf.Sin (theta) * Mathf.Cos (phi);
				y = r * Mathf.Sin (theta) * Mathf.Sin (phi);
				z = r * Mathf.Cos (theta);
				end.setPoint (x, y, z);
				actin.Add (start);
				actin.Add (end);;

				phi = i+1  / 18f * 3.14f;
				theta = j+1  / 18f * 3.14f;
				x = r * Mathf.Sin (theta) * Mathf.Cos (phi);
				y = r * Mathf.Sin (theta) * Mathf.Sin (phi);
				z = r * Mathf.Cos (theta);
				end.setPoint (x, y, z);
				actin.Add (start);
				actin.Add (end);

				phi = i+1  / 18f * 3.14f;
				theta = j-1  / 18f * 3.14f;
				x = r * Mathf.Sin (theta) * Mathf.Cos (phi);
				y = r * Mathf.Sin (theta) * Mathf.Sin (phi);
				z = r * Mathf.Cos (theta);
				end.setPoint (x, y, z);
				actin.Add (start);
				actin.Add (end);*/

		}
	}

	// Update is called once per frame
	void Update ()
	{

	}
}