using System.Collections;
using System.Collections.Generic;
using NUnit.Framework;
using UnityEngine;
using UnityEngine.TestTools;

namespace Tests.Characters
{
    public class CharacterTests : UI.UITest
    {

        /// <summary>
        /// Checking that everything occurs correctly when attacking.
        /// </summary>
        /// <returns></returns>
        [UnityTest]
        public IEnumerator AttackTest1()
        {
            yield return MoveCursor(2, 2);

            yield return Enter();
            yield return DownArrow();

            Assert.AreEqual(new Vector3(2, 1, 0), GameManager.Cursor.transform.position);

            yield return Enter();

            Assert.AreEqual(3, GameManager.CharacterActionMenu.MenuItems.Count);

            Assert.AreEqual(3, GameManager.Cursor.AttackableSpacesWithCharacters.Count);

            GameManager.CharacterActionMenu.OnSubmit();
            yield return null;

            Assert.AreEqual(1, GameManager.ItemSelectionMenu.MenuItems.Count);

            GameManager.ItemSelectionMenu.OnSubmit();
            yield return null;

            yield return Enter();

            Character character = GameManager.CurrentLevel.GetCharacter(0, 1);
            Assert.AreNotEqual(character.MaxHp, character.CurrentHp);
        }

        /// <summary>
        /// Testing to make sure everything is cleaned up after character death
        /// </summary>
        /// <returns></returns>
        [UnityTest]
        public IEnumerator DieTest1()
        {
            ICollection<Character> beforeCharacters = GameManager.CurrentLevel.GetCharacters();

            yield return MoveCursor(2, 2);

            yield return Enter();
            yield return DownArrow();

            yield return Enter();

            GameManager.CharacterActionMenu.OnSubmit();
            yield return null;

            GameManager.ItemSelectionMenu.OnSubmit();
            yield return null;

            yield return Enter();

            yield return MoveCursor(2, 1);

            yield return Enter();
            yield return Enter();

            GameManager.CharacterActionMenu.OnSubmit();
            yield return null;

            GameManager.ItemSelectionMenu.OnSubmit();
            yield return null;

            yield return Enter();

            Assert.IsNull(GameManager.CurrentLevel.GetCharacter(0, 1));

            foreach (Character character in GameManager.CurrentLevel.GetCharacters())
            {
                Assert.IsEmpty(character.MovableSpaces);
                Assert.IsEmpty(character.MovableSpaces);
            }

            ICollection<Character> afterCharacters = GameManager.CurrentLevel.GetCharacters();

            Assert.AreEqual(afterCharacters.Count, beforeCharacters.Count - 1);
        }
    }
}