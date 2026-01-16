using UnityEngine;
using System.Collections;

public class BlobbiManager : MonoBehaviour {
	JellyMesh m_JellyMesh;
	bool col = false;
	private Renderer re;
	private float hue, hueSpeed;
	public bool alive = true;

	// Use this for initialization
	void Start () {
		m_JellyMesh = GetComponent<JellyMesh>();
		re = GetComponent<Renderer>();

		hue = 0f;
		hueSpeed = 0f;
	}
	
	// Update is called once per frame
	void Update() {
		if (alive)
		{
		// Allows for smooth colour changing
		float vel = transform.parent.GetComponent<Rigidbody>().velocity.magnitude;
		hueSpeed += (float)(vel/3.2f) * Time.deltaTime;

		// primitive friction
		hueSpeed *= 0.985f;

		hue += hueSpeed * Time.deltaTime;

		// Value rollover
		if (hue > 1f)
		{
			hue -= 2f;
		} else if (hue < -1f)
		{
			hue += 2f;
		}

		re.material.color = colorFromHSV(Mathf.Abs (hue), 0.8f, 0.9f);

		}
	}

	// Gets the colour of the blob
	public Color getColor()
	{
		return colorFromHSV(Mathf.Abs (hue), 0.8f, 0.9f);
	}

	// Converts a color from HSV values
	Color colorFromHSV(float h, float s, float v)
	{
		// Algorithm adapted from http://cs.rit.edu/~ncs/color/t_convert.html
		int i;
		float f, p, q, t;
		float r, g, b;

		if (s == 0f)
		{
			return new Color(v, v, v);
		}

		h /= (60f/255f);
		i = (int)(Mathf.Floor (h));

		f = h - i;
		p = v * (1 - s);
		q = v * (1 - s * f);
		t = v * (1 - s * (1 - f));

		switch(i)
		{
			case 0:
				r = v;
				g = t;
				b = p;
				break;
			case 1:
				r = q;
				g = v;
				b = p;
				break;
			case 2:
				r = p;
				g = v;
				b = t;
				break;
			case 3:
				r = p;
				g = q;
				b = v;
				break;
			case 4:
				r = t;
				g = p;
				b = v;
				break;
			default:		// case 5:
				r = v;
				g = p;
				b = q;
				break;
		}

		return new Color(r, g, b);

	}

	void FixedUpdate () {
		if (col == false)
		{
			m_JellyMesh.ignoreCollision(transform.parent.GetComponent<Rigidbody>().GetComponent<Collider>(), true);
			col = true;
		}
		m_JellyMesh.SetPosition(transform.parent.position, false);
		//m_JellyMesh.transform.rotation = transform.parent.rotation;
		m_JellyMesh.SetVelocity (transform.parent.GetComponent<Rigidbody>().velocity);
	}

}