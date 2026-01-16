using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine;

namespace Assets.Editor.Tests.Path
{
    public class TestTowerAggro
    {
        [Test]
        public void Test_CompareDistance_NegOne()
        {
            GameObject enemy = new GameObject();
            Attackable enemyAttackable = enemy.AddComponent<Attackable>();
            GameObject enemy2 = new GameObject();
            Attackable enemyAttackable2 = enemy2.AddComponent<Attackable>();

            enemy.transform.SetPositionAndRotation(new Vector3(2, 0, 2), Quaternion.identity);
            enemy2.transform.SetPositionAndRotation(new Vector3(4, 0, 4), Quaternion.identity);

            GameObject tower = new GameObject();
            TowerAggro towerAggro = tower.AddComponent<TowerAggro>();

            int distanceSortValue = towerAggro.CompareDistances(enemyAttackable, enemyAttackable2);

            Assert.That(distanceSortValue, Is.EqualTo(-1));
        }

        [Test]
        public void Test_CompareDistance_Zero()
        {
            GameObject enemy = new GameObject();
            Attackable enemyAttackable = enemy.AddComponent<Attackable>();
            GameObject enemy2 = new GameObject();
            Attackable enemyAttackable2 = enemy.AddComponent<Attackable>();

            enemy.transform.SetPositionAndRotation(new Vector3(2, 0, 2), Quaternion.identity);
            enemy2.transform.SetPositionAndRotation(new Vector3(2, 0, 2), Quaternion.identity);

            GameObject tower = new GameObject();
            TowerAggro towerAggro = tower.AddComponent<TowerAggro>();

            int distanceSortValue = towerAggro.CompareDistances(enemyAttackable, enemyAttackable2);

            Assert.That(distanceSortValue, Is.EqualTo(0));
        }

        [Test]
        public void Test_CompareDistance_PosOne()
        {
            GameObject enemy = new GameObject();
            Attackable enemyAttackable = enemy.AddComponent<Attackable>();
            GameObject enemy2 = new GameObject();
            Attackable enemyAttackable2 = enemy2.AddComponent<Attackable>();

            enemy.transform.SetPositionAndRotation(new Vector3(8, 0, 8), Quaternion.identity);
            enemy2.transform.SetPositionAndRotation(new Vector3(4, 0, 4), Quaternion.identity);

            GameObject tower = new GameObject();
            TowerAggro towerAggro = tower.AddComponent<TowerAggro>();

            int distanceSortValue = towerAggro.CompareDistances(enemyAttackable, enemyAttackable2);

            Assert.That(distanceSortValue, Is.EqualTo(1));
        }

        [Test]
        public void Test_CompareDistance_PosOne2()
        {
            GameObject enemy = new GameObject();
            Attackable enemyAttackable = enemy.AddComponent<Attackable>();
            GameObject enemy2 = new GameObject();
            Attackable enemyAttackable2 = enemy2.AddComponent<Attackable>();

            enemy.transform.SetPositionAndRotation(new Vector3(8, 0, -8), Quaternion.identity);
            enemy2.transform.SetPositionAndRotation(new Vector3(-4, 0, 4), Quaternion.identity);

            GameObject tower = new GameObject();
            TowerAggro towerAggro = tower.AddComponent<TowerAggro>();

            int distanceSortValue = towerAggro.CompareDistances(enemyAttackable, enemyAttackable2);

            Assert.That(distanceSortValue, Is.EqualTo(1));
        }

        [Test]
        public void Test_AddTarget()
        {
            GameObject enemy = new GameObject();
            Attackable enemyAttackable = enemy.AddComponent<Attackable>();
            SphereCollider enemyCollider = enemy.AddComponent<SphereCollider>();
            
            enemy.transform.SetPositionAndRotation(new Vector3(8, 0, -8), Quaternion.identity);
            
            enemyCollider.radius = 0.1f;

            GameObject tower = new GameObject();
            TowerAggro towerAggro = tower.AddComponent<TowerAggro>();
            SphereCollider towerCollider = tower.AddComponent<SphereCollider>();

            towerCollider.radius = 4;
            towerAggro.Start();
            towerAggro.OnTriggerEnter(enemyCollider);

            Assert.That(towerAggro.Targets.Count, Is.EqualTo(1));
        }

        [Test]
        public void Test_LoseTarget()
        {
            GameObject enemy = new GameObject();
            Attackable enemyAttackable = enemy.AddComponent<Attackable>();
            SphereCollider enemyCollider = enemy.AddComponent<SphereCollider>();

            enemy.transform.SetPositionAndRotation(new Vector3(8, 0, -8), Quaternion.identity);

            enemyCollider.radius = 0.1f;

            GameObject tower = new GameObject();
            TowerAggro towerAggro = tower.AddComponent<TowerAggro>();
            SphereCollider towerCollider = tower.AddComponent<SphereCollider>();

            towerCollider.radius = 4;
            towerAggro.Start();
            towerAggro.OnTriggerEnter(enemyCollider);

            towerAggro.OnTriggerExit(enemyCollider);

            Assert.That(towerAggro.Targets.Count, Is.EqualTo(0));
        }



        [Test]
        public void Test_GetTarget()
        {
            GameObject enemy = new GameObject();
            Attackable enemyAttackable = enemy.AddComponent<Attackable>();
            SphereCollider enemyCollider = enemy.AddComponent<SphereCollider>();

            enemy.transform.SetPositionAndRotation(new Vector3(8, 0, -8), Quaternion.identity);
            enemyCollider.radius = 0.1f;

            GameObject tower = new GameObject();
            TowerAggro towerAggro = tower.AddComponent<TowerAggro>();
            SphereCollider towerCollider = tower.AddComponent<SphereCollider>();

            towerCollider.radius = 4;
            towerAggro.Start();
            towerAggro.OnTriggerEnter(enemyCollider);

            enemy = new GameObject();
            enemyAttackable = enemy.AddComponent<Attackable>();
            enemyCollider = enemy.AddComponent<SphereCollider>();

            enemy.transform.SetPositionAndRotation(new Vector3(4, 0, 4), Quaternion.identity);
            enemyCollider.radius = 0.1f;
            towerAggro.OnTriggerEnter(enemyCollider);

            enemy = new GameObject();
            enemyAttackable = enemy.AddComponent<Attackable>();
            enemyCollider = enemy.AddComponent<SphereCollider>();

            enemy.transform.SetPositionAndRotation(new Vector3(2, 0, 4), Quaternion.identity);
            enemyCollider.radius = 0.1f;
            towerAggro.OnTriggerEnter(enemyCollider);

            enemy = new GameObject();
            enemyAttackable = enemy.AddComponent<Attackable>();
            enemyCollider = enemy.AddComponent<SphereCollider>();

            enemy.transform.SetPositionAndRotation(new Vector3(1, 0, 1), Quaternion.identity);
            enemyCollider.radius = 0.1f;
            towerAggro.OnTriggerEnter(enemyCollider);

            Attackable target = towerAggro.GetTarget();

            Assert.That(target.GetInstanceID(), Is.EqualTo(enemyAttackable.GetInstanceID()));
        }
    }
}