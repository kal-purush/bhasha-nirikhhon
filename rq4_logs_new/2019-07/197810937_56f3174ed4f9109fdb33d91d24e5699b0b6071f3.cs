using UnityEngine;
using static Constants;

public class BossShipSuper : BossShip
{

	private float tweenTime;
	
	private readonly float speed = BOSS_SPEED;
	private readonly float xScale = BOSS_X_PATH_SCALE;
	private readonly float yScale = BOSS_Y_PATH_SCALE;

	protected override void Awake()
	{
		base.Awake();
	}

	/// <summary>
	/// Метод начинает атаку босса на игрока и он активирует все свои компоненты
	/// После активации босс перестает быть неуязвимым
	/// </summary>
	public override void StartAttackPhase()
	{
		base.StartAttackPhase();
	}



	/// <summary>
	/// Переопределенный метод BaseShip для стрельбы
	/// </summary>
	public override void Shoot()
	{
		if (attackPhase)
		{
			base.Shoot();
		}
	}

	void Update()
	{
		if (attackPhase)
		{
			tweenTime += Time.deltaTime;
			Vector3 upVector = Vector3.up * Mathf.Sin(tweenTime * speed);
			Vector3 rightVector = Vector3.right * Mathf.Sin(tweenTime / 2 * speed);
			transform.position = startPos + (rightVector * xScale) - (upVector * yScale);
		}
	}
}