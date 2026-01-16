using System.Collections;
using NUnit.Framework;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.TestTools;

namespace Tests
{
    public class MainMenuTest
    {

        private MainMenu MainMenu;

        [UnitySetUp]
        public IEnumerator UnitySetUp()
        {
            yield return SceneManager.LoadSceneAsync("MainMenu", LoadSceneMode.Single);
        }

        [SetUp]
        public void Setup()
        {
            MainMenu = Object.FindObjectOfType<MainMenu>();
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

            Scene scene = SceneManager.GetSceneByName("FlameSymbol");
            Assert.IsTrue(scene.isLoaded);
        }
    }
}