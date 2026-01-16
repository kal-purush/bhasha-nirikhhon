using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using EditorUtilities;

namespace Chalkling
{
	public class ChalklingCore : MonoBehaviour
	{
		[SerializeField] private int maxHP;
		[SerializeField] private float speed;
		[SerializeField] private int currHP;

		ChalklingMovement movement;

		private void Awake()
		{
			movement = GetComponent<ChalklingMovement>();
		}

		private void Start()
		{
			currHP = maxHP;
			movement._moveSpeed = speed;
		}
	}

}