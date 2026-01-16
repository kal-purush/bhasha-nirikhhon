using System.Collections;
using NUnit.Framework;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.TestTools;

namespace Tests
{
    public class MainMenuTests
    {

        private MainMenu MainMenu;

        [UnitySetUp]
        public IEnumerator UnitySetUp()
        {
            yield return SceneManager.LoadSceneAsync(GameManager.SceneNameMainMenu, LoadSceneMode.Single);
        }

        [SetUp]
        public void Setup()
        {
            MainMenu = Object.FindObjectOfType<MainMenu>();
        }

        [TearDown]
        public void TearDown()
        {
            if (MainMenu != null)
            {
                Object.Destroy(MainMenu.gameObject);
            }

            GameManager gameManager = Object.FindObjectOfType<GameManager>();
            if (gameManager != null)
            {
                Object.Destroy(gameManager.gameObject);
            }
        }

        [Test]
        public void MainMenuSetupTest1()
        {
            Assert.AreEqual(0, MainMenu.ButtonIndex);
        }

        [UnityTest]
        public IEnumerator NewGameButtonTest1()
        {
            MainMenu.NewGameButtonOnClick();

            yield return null;

            Assert.AreEqual(GameManager.SceneNameFlameSymbol, SceneManager.GetActiveScene().name);

            // Make sure main menu is destroyed
            MainMenu mainMenu = Object.FindObjectOfType<MainMenu>();
            Assert.Null(mainMenu);
        }
    }
}