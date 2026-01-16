#nullable enable
using Daipan.Battle.interfaces;
using Daipan.Enemy.Scripts;
using Daipan.LevelDesign.Enemy.Scripts;
using Daipan.Player.Scripts;
using NUnit.Framework;

public class EnemyAttackTest
{
    [Test]
    public void IHpSetterShouldDecrease10WithAttack()
    {
        // Arrange
        var player = new DummyPlayer() { CurrentHp = 100 };
        var enemyParamData = new EnemyParamData()
        {
            EnemyEnum = () => EnemyEnum.None,
            GetAttackAmount = () => 10
        };
        var enemyAttack = new EnemyAttack(enemyParamData);

        // Act
        enemyAttack.Attack(player);

        // Assert
        Assert.AreEqual(90, player.CurrentHp);
    }
    class DummyPlayer : IHpSetter
    {
        public int CurrentHp { get; set; }
    }
}