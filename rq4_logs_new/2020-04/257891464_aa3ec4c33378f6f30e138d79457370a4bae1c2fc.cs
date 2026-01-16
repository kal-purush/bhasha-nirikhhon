using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ProtectionFromRain : TemporalSingleton<ProtectionFromRain>
{
	[SerializeField] private float m_maxLife = 0f;
	[SerializeField] private float m_lifeDepleteRatePerSecond = 0f;
	[SerializeField] private float m_lifeAddedPerLap = 0f;
	private float m_currentLife = 0f;
	private float m_lifePercentage = 0f;
	private float maxSizeX = 0f;
	private float maxSizeY = 0f;

	private Transform m_protectionTransform = null;
	[SerializeField] private float m_scaleChangeSpeed = 0f;
	private Vector3 m_maxScale = Vector3.zero;
	private Vector3 m_currentScale = Vector3.zero;

	// Start is called before the first frame update
	public override void Awake()
	{
		base.Awake();
		m_protectionTransform = this.transform;
		m_currentLife = 0;
		m_maxScale = m_protectionTransform.localScale;
		m_protectionTransform.localScale = Vector3.zero;
		m_currentScale = m_protectionTransform.localScale;
	}

	// Update is called once per frame
	void Update()
	{
		DealDamageToProtection(m_lifeDepleteRatePerSecond * Time.deltaTime);

		m_lifePercentage = m_currentLife / m_maxLife;

		if (m_lifePercentage > 1)
		{
			m_lifePercentage = 1;
		}
		if (m_lifePercentage < 0)
		{
			m_lifePercentage = 0;
		}

		float currentSizeX = m_lifePercentage * m_maxScale.x;
		float currentSizeY = m_lifePercentage * m_maxScale.y;

		m_currentScale = Vector3.MoveTowards(m_currentScale, new Vector3(currentSizeX, currentSizeY, 1), m_scaleChangeSpeed * Time.deltaTime);

		m_protectionTransform.localScale = m_currentScale;
	}

	void DealDamageToProtection(float damage)
	{
		m_currentLife = m_currentLife - Mathf.Abs(damage);
		if (m_currentLife < 0)
		{
			m_currentLife = 0;
		}
	}

	public void HealProtection()
	{
		m_currentLife = m_currentLife + Mathf.Abs(m_lifeAddedPerLap);
		if (m_currentLife > m_maxLife)
		{
			m_currentLife = m_maxLife;
		}
	}
}