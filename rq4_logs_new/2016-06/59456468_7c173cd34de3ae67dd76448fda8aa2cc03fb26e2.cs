using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class PinchFrame : MonoBehaviour {

	[SerializeField, Header("ピンチ処理開始サイズ")]
	float m_pinchStartSize = 50;


	[SerializeField, Header("最長点滅間隔(秒)")]
	float m_flashingMaxTime = 1;
	
	[SerializeField, Header("最短点滅間隔(秒)")]
	float m_flashingMinTime = 0.1f;
	// フラッシュ中の計測時間
	float m_flashTime;
	float m_alphaVariable;  // アルファ値の変動値

	enum FlashState
	{
		ON,
		OFF
	}
	FlashState m_flashState = FlashState.ON;

	Image m_frame;

	PlayerCharacterController m_player;
	// Use this for initialization
	void Start () {
		m_frame = GetComponent<Image>();
		m_frame.color = new Color(m_frame.color.r, m_frame.color.g, m_frame.color.b, 0);

		//m_frame.enabled = false;
	  //  m_frame.rectTransform.sizeDelta = new Vector2(Screen.width, Screen.height);
		m_flashTime = m_flashingMaxTime;
		m_player = GameObject.Find("PlayerCharacter").GetComponent<PlayerCharacterController>();
	}
	
	// Update is called once per frame
	void Update ()
	{
		FlashProcesss();
	}


	void FlashProcesss()
	{
		if (m_player.size <= m_pinchStartSize && m_player.size > 0) 
		{
			m_flashTime -= Time.deltaTime;
			switch (m_flashState)
			{
				case FlashState.ON:
					if (m_flashTime <= 0)
					{
						m_flashState = FlashState.OFF;
					//	 m_frame.enabled = false;
						m_flashTime = Mathf.Lerp(m_flashingMinTime, m_flashingMaxTime, m_player.size / m_pinchStartSize);
						m_alphaVariable = 1.0f / m_flashTime;
					}
                    m_frame.color -= new Color(0, 0, 0, m_alphaVariable * Time.deltaTime);
					
					break;
				case FlashState.OFF:
					if (m_flashTime <= 0)
					{
						m_flashState = FlashState.ON;
						m_flashTime = Mathf.Lerp(m_flashingMinTime, m_flashingMaxTime, m_player.size / m_pinchStartSize);
						m_alphaVariable = 1.0f / m_flashTime;
					}
                    m_frame.color += new Color(0, 0, 0, m_alphaVariable * Time.deltaTime);
					
					break;
				default:
					break;

			}

		}
		else if (m_player.size <= 0)
		{
			m_frame.color = new Color(m_frame.color.r, m_frame.color.g, m_frame.color.b, 1);

		}
		else if (m_player.size > m_pinchStartSize)
		{
			m_frame.color = new Color(m_frame.color.r, m_frame.color.g, m_frame.color.b, 0);
            m_flashTime = 0; 
            m_alphaVariable = 1.0f / m_flashTime;

            FlashState m_flashState = FlashState.ON;
		
		}
	}
}