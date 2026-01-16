using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Moth : MonoBehaviour
{
	[SerializeField] private float m_radiusMothInteract = 0f;
	[SerializeField] private float m_healAmmountToCandle = 0f;
	private Vector3 m_maxScale = Vector3.zero;
	private Vector3 m_currentScale = Vector3.zero;

	[SerializeField] private float m_scaleSpeed = 0f;
	private bool m_grabbed = false;
	private bool m_growing = false;
    
	// Start is called before the first frame update
    void Start()
    {
		m_maxScale = this.transform.localScale;
		this.transform.localScale = Vector3.zero;
		m_currentScale = this.transform.localScale;
		m_growing = true;
	}

    // Update is called once per frame
    void Update()
    {
		if (!m_grabbed && m_growing)
		{
			m_currentScale = Vector3.MoveTowards(m_currentScale, m_maxScale, m_scaleSpeed * Time.deltaTime);
			if(Vector3.Distance(m_currentScale, m_maxScale) == 0)
			{
				m_growing = false;
			}
			this.transform.localScale = m_currentScale;
		}
		else if (m_grabbed)
		{
			Vector3 mouse = Camera.main.ScreenToWorldPoint(Input.mousePosition);
			this.transform.position = new Vector3(mouse.x, mouse.y, this.transform.position.z);
		}
		print(m_grabbed);
    }

	private void OnMouseDown()
	{
		print(m_grabbed);
		m_grabbed = true;
	}

	private void OnMouseUp()
	{
		m_grabbed = false;

		Collider2D[] hit = Physics2D.OverlapCircleAll(this.transform.position,m_radiusMothInteract);

		for (int i = 0; i < hit.Length; i++)
		{
			if (hit[i].gameObject.GetComponent<CandleBehaviour>() != null)
			{
				hit[i].gameObject.GetComponent<CandleBehaviour>().HealCandle(m_healAmmountToCandle);
				Destroy(this.gameObject);
				break;
			}
			else if (hit[i].gameObject.GetComponent<Finger>() != null)
			{
				hit[i].gameObject.GetComponent<Finger>().ScareFinger();
				Destroy(this.gameObject);
				break;
			}
			else if(hit[i].gameObject.GetComponent<Spawner>() != null)
			{
				Destroy(this.gameObject);
			  break;
			
			}
			//else if (hit[i].gameObject.GetComponent<>() != null)
			//{
			//  break;
			//}
		}


	}
}