using UnityEngine;
using System.Collections.Generic;
using System.IO;
using System.Collections;

public class Factin : MonoBehaviour
{
	class Point
	{
		public float x;
		public float y;
		public float z;
			
		public void setPoint (float x, float y, float z)
		{
			this.x = x;
			this.y = y;
			this.z = z;
		}
	}



	int i, j, k, f;
	float r1 = 621.09f, r2 = 690.10f, r, theta, phi, layer1, layer2;
	List<Point> actin = new List<Point> ();

	// Use this for initialization
	void Start ()
	{
	

		layer1 = (r2 - r1) / 3f + r1;
		layer2 = 2f * (r2 - r1) / 3f + r1;
		//for (j=0; j<19; j++) {
		j = 1;
		for (i=0; i<36; i++) {
			Point start = new Point ();
			Point end = new Point ();
			r = r1;
			phi = i / 18f * 3.14f;
			theta = j / 18f * 3.14f;
			float x = r * Mathf.Sin (theta) * Mathf.Cos (phi);
			float y = r * Mathf.Sin (theta) * Mathf.Sin (phi);
			float z = r * Mathf.Cos (theta);
			start.setPoint (x, y, z);

			r = layer1;
			phi = i / 18f * 3.14f;
			theta = j / 18f * 3.14f;
			x = r * Mathf.Sin (theta) * Mathf.Cos (phi);
			y = r * Mathf.Sin (theta) * Mathf.Sin (phi);
			z = r * Mathf.Cos (theta);
			end.setPoint (x, y, z);
			actin.Add (start);
			actin.Add (end);
		

		
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
		LineRenderer renderer = gameObject.GetComponent<LineRenderer> ();
		renderer.SetWidth (3f, 3f);
		renderer.material.color = Color.blue;
		renderer.SetVertexCount(actin.Count);
		for (int h=0; h<actin.Count; h++) {
				Vector3 path = new Vector3 (actin [h].x, actin [h].y, actin [h].z);
				renderer.SetPosition (h, path);
		
	}
	}


//	}

				
			
		
		
	// Update is called once per frame
	void Update ()
	{
		
	}
}