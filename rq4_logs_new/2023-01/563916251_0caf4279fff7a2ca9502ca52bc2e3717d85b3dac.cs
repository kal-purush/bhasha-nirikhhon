using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Bonus : SimpleGameStateObserver
{
    Rigidbody m_Rigidbody;
	Transform m_Transform;

    [Header("Life duration")]
	[SerializeField]
	private float m_LifeDuration;

	[Header("Movement")]
	[SerializeField]
	private float m_TranslationSpeed;

    protected override void Awake()
	{
		base.Awake();
		m_Rigidbody = GetComponent<Rigidbody>();
		m_Transform = GetComponent<Transform>();
		Destroy(gameObject, m_LifeDuration);
	}

    private void FixedUpdate()
	{
		if (!GameManager.Instance.IsPlaying) return;

		Vector3 moveVect = m_Transform.right * m_TranslationSpeed * Time.fixedDeltaTime;
		m_Rigidbody.MovePosition(m_Rigidbody.position + moveVect);
	}

    private void OnCollisionEnter(Collision collision)
	{
		Destroy(gameObject);
	}

    protected override void GameMenu(GameMenuEvent e)
	{
		Destroy(gameObject);
	}

	protected override void GameOver(GameOverEvent e)
	{
		Destroy(gameObject);
	}

	protected override void GameVictory(GameVictoryEvent e)
	{
		Destroy(gameObject);
	}
}