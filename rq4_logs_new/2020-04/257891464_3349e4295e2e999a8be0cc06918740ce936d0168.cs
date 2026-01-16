using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CandleBehaviour : MonoBehaviour
{
	[SerializeField] private float m_maxLife = 0f;
	[SerializeField] private float m_lifeDepleteRatePerSecond = 0f;
	[SerializeField] private int m_timeInSecondsToReachMaxDepeleteRate = 0;
	private float m_lifeDepleteRateAccelerationPerSecond = 0f;
	[SerializeField] private float m_maxLifeDepleteRatePerSecond = 0f;
	private float m_currentLife = 0f;

	private float m_lifePercentage = 0f;
	private float maxSizeX = 0f;
	private float maxSizeY = 0f;

	[SerializeField] private Transform m_llamaSpriteTransform = null;
	[SerializeField] private float m_scaleChangeSpeed = 0f;
	private Vector3 m_maxLlamaScale = Vector3.zero;
	private Vector3 m_currentLlamaScale = Vector3.zero;
    // Start is called before the first frame update
    void Awake()
    {
		m_lifeDepleteRateAccelerationPerSecond = (m_maxLifeDepleteRatePerSecond - m_lifeDepleteRatePerSecond)/ (float)m_timeInSecondsToReachMaxDepeleteRate;
		m_currentLife = m_maxLife;
		m_maxLlamaScale = m_llamaSpriteTransform.localScale;
		m_currentLlamaScale = m_maxLlamaScale;
	}

	// Update is called once per frame
	void Update()
	{
		if(m_lifeDepleteRatePerSecond < m_maxLifeDepleteRatePerSecond)
		{
			m_lifeDepleteRatePerSecond = m_lifeDepleteRatePerSecond + m_lifeDepleteRateAccelerationPerSecond * Time.deltaTime;
			if(m_lifeDepleteRatePerSecond > m_maxLifeDepleteRatePerSecond)
			{
				m_lifeDepleteRatePerSecond = m_maxLifeDepleteRatePerSecond;
			}
		}

		DealDamageToCandle(m_lifeDepleteRatePerSecond * Time.deltaTime);		

		m_lifePercentage = m_currentLife / m_maxLife;
		
		if(m_lifePercentage > 1)
		{
			m_lifePercentage = 1;
		}
		if(m_lifePercentage < 0)
		{
			m_lifePercentage = 0;
		}

		float currentSizeX = m_lifePercentage * m_maxLlamaScale.x;
		float currentSizeY = m_lifePercentage * m_maxLlamaScale.y;

		m_currentLlamaScale = Vector3.MoveTowards(m_currentLlamaScale, new Vector3(currentSizeX, currentSizeY, 1), m_scaleChangeSpeed * Time.deltaTime);

		m_llamaSpriteTransform.localScale = m_currentLlamaScale;

		if (Input.GetKeyDown(KeyCode.I))
		{
			DealDamageToCandle(10);
		}
		if (Input.GetKeyDown(KeyCode.O))
		{
			HealCandle(50);
		}
	}

	void DealDamageToCandle(float damage)
	{
		m_currentLife = m_currentLife - Mathf.Abs(damage);
		if(m_currentLife < 0)
		{
			m_currentLife = 0;
		}
	}

	void HealCandle(float healAmmount)
	{
		m_currentLife = m_currentLife + Mathf.Abs(healAmmount);
		if(m_currentLife > m_maxLife)
		{
			m_currentLife = m_maxLife;
		}

	}
}