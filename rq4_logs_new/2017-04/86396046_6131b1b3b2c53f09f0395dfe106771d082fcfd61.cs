using Spriter2UnityDX;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine;

class BossBlackBishop : BasicEnnemy
{
    [HideInInspector]
    public List<Rocket> Rockets = new List<Rocket>();
    public float StopPosition = 0;
    protected int LifeMax = 0;
    protected bool IsInvisible = false;
    protected Vector3[] SpawnPosition = new Vector3[7];
    protected float size = 0;
    protected int spawn = 0;
    CombinationGenerator combinationGenerator = new CombinationGenerator();
    EntityRenderer Rend;

    protected new void Start()
    {
        base.Start();
        Rend = GetComponent<EntityRenderer>();
        size = GetComponent<BoxCollider>().size.x;
        RectTransform TopPos = GameObject.Find("Canvas/TopBackground").GetComponent<RectTransform>();
        RectTransform BottomPos = GameObject.Find("Canvas/ButtonBackground").GetComponent<RectTransform>();
        StopPosition = TopPos.position.y + BottomPos.position.y / 9 - boxCollider.size.y * 0.9f;
        if (Gm.WaveManager.EnemyMax < 6)
            Gm.WaveManager.EnemyMax = 6;
        LifeMax = Life;
        UpdateSpawnPosition();
    }

    public override List<CombinationHandler.Button> getCombination()
    {
        if (!IsInvisible)
            return Combination;
        return new List<CombinationHandler.Button>();
    }

    protected override void Move()
    {
        if (IsMoving)
        {
            if (Position.y < StopPosition + Speed && Position.y > StopPosition - Speed)
            {
                Speed = 0;
            }
            base.Move();
        }
    }

    public override void DecreaseLifePoint(int lp)
    {
        combinationGenerator.FixedSize = CombinationSize;
        Combination = combinationGenerator.GetListButton();
        ResetCombination();
        base.DecreaseLifePoint(lp);
        if (Life == LifeMax / 3)
            StartInvisible(1, 3);
        if (Life == LifeMax / 3 * 2)
            StartInvisible(2, 1);
    }

    protected void StartInvisible(int Size, int Life)
    {
        foreach (GameObject b in ButtonsEnemy)
        {
            b.SetActive(false);
        }
        EnemyLifeObj.SetActive(false);
        Rend.enabled = false;
        IsInvisible = true;
        while (Rockets.Count > 0)
            Rockets[0].Explode();
        for (int i = 0; i < 5; ++i)
        {
            spawn += 1;
            Spawn(Size, Life);
        }
    }

    protected void Spawn(int Size, int Life)
    {
        ConfigurationEnemy m = new ConfigurationEnemy();
        m.Speed = 0.2f;
        m.t = ConfigurationEnemy.Type.Boss;
        m.SpawnCoolDown = 0;
        m.name = "Rocket";
        m.Gold = 0;
        m.CombinationSize = Size;
        m.Life = Life;
        m.prefab = ConfigurationGame.instance.GenerateEnemy("Rocket");
        Gm.WaveManager.Spawn(m);
    }

    public Vector3 addRocket(Rocket R)
    {
        spawn -= 1;
        Rockets.Add(R);
        if (IsInvisible)
            return new Vector3(SpawnPosition[Rockets.Count - 1].x + Rockets[Rockets.Count - 1].size * 4,
                SpawnPosition[Rockets.Count - 1].y,
                SpawnPosition[Rockets.Count - 1].z);
        if (Rockets.Count == 1 || Rockets[0].Position.x == SpawnPosition[6].x)
            return SpawnPosition[5];
        else
            return SpawnPosition[6];
    }

    public void RocketDeath(Rocket R)
    {
        Rockets.Remove(R);
    }

    protected void UpdateSpawnPosition()
    {
        RectTransform Canvas = GameObject.Find("Canvas").GetComponent<RectTransform>();
        float totalSize = Screen.width * Canvas.localScale.x;
        for (int i = 0; i < 5; ++i)
        {
            SpawnPosition[i] = new Vector3(totalSize / 5 * i - totalSize / 2, StopPosition, Position.z - 1);
        }
        SpawnPosition[5] = new Vector3(Position.x - size, StopPosition, Position.z - 1);
        SpawnPosition[6] = new Vector3(Position.x + size, StopPosition, Position.z - 1);
    }

    protected override void Update()
    {
        base.Update();
        if (IsInvisible)
        {
            if (spawn == 0 && Rockets.Count == 0)
            {
                foreach (GameObject b in ButtonsEnemy)
                {
                    b.SetActive(true);
                }
                EnemyLifeObj.SetActive(true);
                Rend.enabled = true;
                IsInvisible = false;
            }
        }
        else
        {
            if (Rockets.Count + spawn < 2)
                StartCoroutine("LaunchAttack", UnityEngine.Random.Range(0, 3));
        }
    }

    protected IEnumerator LaunchAttack(float delay)
    {
        spawn += 1;
        float TimeUntilSummon = delay;

        while (TimeUntilSummon > 0)
        {
            TimeUntilSummon -= Time.deltaTime;
            yield return null;
        }
        if (IsInvisible)
        {
            spawn -= 1;
            yield break;
        }
        Spawn(3, 1);
    }

    public override void Die()
    {
        while (Rockets.Count > 0)
            Rockets[0].Explode();
        base.Die();
    }
}