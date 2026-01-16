using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AdvLevelUpSystem : MonoBehaviour {

	public int level;
	public long currentExp;

	public float xpForNextLevel;
	public float xpDiffenceToNextLevel;
	public float totalXpDifference;

	public float fillAmount;
	public float reverseFillAmount;

	public int statPoints;
	public int skillPoints;
	void Start () 
	{
		InvokeRepeating("AddExp", 1f, 0.5f);
	}

	void AddExp()
	{
		CalculateLevel(5);
		//level = GetLevel(currentExp);
	}
	
	// Update is called once per frame
	void Update () 
	{
		if (Input.GetKeyDown(KeyCode.Space))
		{
			print("Level" + level + " requires: " + GetExp(level) + " EXP");
		}
	}

	long GetExp(int level)
	{
		float expMultiplier = Mathf.Pow(level - 1, 2.5f);

		float baseExp = 1000f;
		float p = 10;
		float t = 0;

		for (int i = 1; i < level; i++)
		{
			if (i == 43)
				t = 1f;
			if (i == 83)
				t = 4f;
			if (i == 123)
				t = 16f;
			if (i == 143)
				t = 64f;
			if (i == 163)
				t = 256f;
			if (i == 183)
				t = 1024f;
			if (i == 193)
				t = 4096f;
			p += t;
			baseExp += p;
		}
		return (long)(baseExp * expMultiplier);
	}

	int GetLevel(long currentExp)
	{
		float baseExp = 1000f;

		float p = 10;
		float t = 0;

		for (int i = 1; i < level; i++)
		{
			if (i == 43)
				t = 1f;
			if (i == 83)
				t = 4f;
			if (i == 123)
				t = 16f;
			if (i == 143)
				t = 64f;
			if (i == 163)
				t = 256f;
			if (i == 183)
				t = 1024f;
			if (i == 193)
				t = 4096f;
			p += t;
			baseExp += p;
		}

		float temp_expMultiplier = currentExp / baseExp;

		float whateverExp = Mathf.Pow(temp_expMultiplier, 1f /2.5f);

		float temp_level = whateverExp+1;

		return (int)temp_level;
	}

	void CalculateLevel(int amount)
	{
		currentExp += amount;

		level = GetLevel(currentExp);

		xpForNextLevel = GetExp(level + 1);
		xpDiffenceToNextLevel = xpForNextLevel - currentExp;
		totalXpDifference = xpForNextLevel - GetExp(level);

		fillAmount = (float)xpDiffenceToNextLevel / (float)totalXpDifference;
		reverseFillAmount = 1 - fillAmount;

		statPoints = 5 * (level-1);
		skillPoints = 15 * (level-1);
	}
}