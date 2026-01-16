using System.Collections;
using NUnit.Framework;
using PersonalDevelopment;
using UnityEngine;
using UnityEngine.InputSystem;
using UnityEngine.SceneManagement;
using UnityEngine.TestTools;

namespace Tests
{
    public class BotLongRangeSandboxTests : InputTestFixture
    {
        private Keyboard _keyboard = null;
        private PlayerInput _inputOne = null;
        private GameObject _playerOne = null;

        [SetUp]
        public override void Setup()
        {
            base.Setup();
            SceneManager.LoadScene("Bot_LongRange_Sandbox");
            _keyboard = InputSystem.AddDevice<Keyboard>();
        }
        
        private void PlayerInputSetup()
        {
            var players = PlayerInput.all;
            _playerOne = players[0].gameObject;
            _inputOne = players[0];
            _inputOne.SwitchCurrentControlScheme("Keyboard&Mouse_Keys", Keyboard.current);
            
        }
        

        // A UnityTest behaves like a coroutine in Play Mode. In Edit Mode you can use
        // `yield return null;` to skip a frame.
        [UnityTest]
        public IEnumerator BotLongRange_StatePatrolToAttack_SameXPosition()
        {
            PlayerInputSetup();
            
            // When
            _playerOne.transform.position = new Vector3(-2, 1, 0);
            var bot = GameObject.FindObjectOfType<BotMovementBehaviour>();
            bot.gameObject.transform.position = new Vector3(3, 1, 0);
            bot.Initialize(bot.GetComponent<Rigidbody>(), new DummyEnemyProperties(), true);

            //Then
            yield return new WaitForSeconds(2f);
            var xPositionBefore = bot.gameObject.transform.position.x;
            yield return new WaitForSeconds(2f);
            var xPositionAfter = bot.gameObject.transform.position.x;
            
            //Therefore
            Assert.AreEqual(xPositionBefore, xPositionAfter, "X position should be the same after detecting the player and staying still");
        }

        [UnityTest]
        public IEnumerator BotLongRange_AttackToPatrolMovingOutOfDetection_DifferentXPosition()
        {
            PlayerInputSetup();
            
            // When
            _playerOne.transform.position = new Vector3(-2, 1, 0);
            var bot = GameObject.FindObjectOfType<BotMovementBehaviour>();
            bot.gameObject.transform.position = new Vector3(0, 1, 0);
            var properties = new DummyEnemyProperties();
            properties.moveDistance = 2;
            bot.Initialize(bot.GetComponent<Rigidbody>(), properties, false);

            //Then
            var botPositionBefore = bot.gameObject.transform;
            yield return new WaitForSeconds(4f);
            Assert.AreEqual(bot.gameObject.transform, botPositionBefore);
            Press(_keyboard.leftArrowKey);
            yield return new WaitForSeconds(1f);
            var xPositionBefore = bot.gameObject.transform.position.x;
            yield return new WaitForSeconds(2f);
            var xPositionAfter = bot.gameObject.transform.position.x;
            
            //Therefore
            Assert.AreNotEqual(xPositionBefore, xPositionAfter, "X position should be the same after detecting the player and staying still");
        }

        private class DummyEnemyProperties : IEnemyProperties
        {
            public float moveDistance = 5;
            public float moveSpeed = 10;
            public float pushBackHorizontal = 100;
            public float pushBackVeritcal = 100;
            
            public float MoveDistance()
            {
                return moveDistance;
            }

            public float MoveSpeed()
            {
                return moveSpeed;
            }

            public float PushBackHorizontal()
            {
                return pushBackHorizontal;
            }

            public float PushBackVertical()
            {
                return pushBackVeritcal;
            }
        }
    }
}