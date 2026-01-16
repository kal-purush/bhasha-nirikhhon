using System.Collections;
using System.Collections.Generic;
using NUnit.Framework;
using UnityEngine;
using UnityEngine.TestTools;

namespace Tests
{
    public class LevelTests : UI.UITest
    {
        [UnityTest]
        public IEnumerator CheckColor()
        {
            yield return null;

            Character character = GameManager.CurrentLevel.GetCharacter(0, 0);

            SpriteRenderer spriteRenderer = Level.FindComponentInChildWithTag<SpriteRenderer>(character.gameObject, Level.CharacterColorTag);
            Assert.AreEqual(Color.red, spriteRenderer.color);
        }
    }
}