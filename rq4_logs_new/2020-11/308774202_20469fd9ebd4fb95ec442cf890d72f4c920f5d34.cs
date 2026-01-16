using System.Collections;
using NUnit.Framework;
using PersonalDevelopment;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.TestTools;

namespace Tests
{
    public class MenuSceneTests: SnapshotTestCase
    {

        [SetUp]
        protected override void Setup()
        {
            RecordMode = false;
            base.Setup();
            SceneManager.LoadScene("Menu");
        }
        
        [UnityTest]
        public IEnumerator TestMenuSnapShotOnStart()
        {
            // Wait for 4 frames
            for (int i = 0; i < 4; i++)
            {
                yield return new WaitForEndOfFrame();
            }

            SnapshotVerifyView();
        }
        
        [UnityTest]
        public IEnumerator TestMenuSnapShotOnPlayerJoin()
        {
            // Wait for 4 frames
            for (int i = 0; i < 4; i++)
            {
                yield return new WaitForEndOfFrame();
            }
            
            var stateManager = StateManager.Instance;
            stateManager.SetState(State.CharacterSelection);

            // Wait for 4 frames
            for (int i = 0; i < 4; i++)
            {
                yield return new WaitForEndOfFrame();
            }
            
            SnapshotVerifyView();
        }
    }
}