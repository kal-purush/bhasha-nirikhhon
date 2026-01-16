using SDD.Events;
using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemyBoss : Enemy
{
  
	iTweenPath m_Path;
	Vector3[] m_PathNodes;
	float m_PathLength;
	float m_Distance;

	public void InitPath(iTweenPath path)
	{
		m_Path = path;
		m_PathNodes = m_Path.nodes.ToArray();
		m_PathLength = iTween.PathLength(m_PathNodes);

		m_Distance = 0;
	}

	protected override Vector3 MoveVect
	{
		get
		{
			Vector3 nextPosition = iTween.PointOnPath(m_PathNodes, m_Distance / m_PathLength);
			return nextPosition - m_Transform.position;
		}
	}

	public override void FixedUpdate()
	{
		if (m_Destroyed) return;

		m_Distance += m_TranslationSpeed * Time.fixedDeltaTime;
		if(m_Distance>m_PathLength)
		{
			EventManager.Instance.Raise(new EnemyHasBeenDestroyedEvent() { eEnemy = this, eDestroyedByPlayer = false });
			m_Destroyed = true;
			Destroy(gameObject);
		}
		else
			base.FixedUpdate();
	}
}