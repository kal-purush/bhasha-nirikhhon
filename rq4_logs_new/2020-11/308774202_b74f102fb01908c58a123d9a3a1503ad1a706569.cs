using System.Collections;
using NUnit.Framework;
using PersonalDevelopment;
using UnityEngine;
using UnityEngine.InputSystem;
using UnityEngine.SceneManagement;
using UnityEngine.TestTools;

namespace Tests
{
    public class CharacterControlMultiplayerTests : InputTestFixture
    {
        private Keyboard _keyboard = null;
        private PlayerInput _inputOne = null;
        private PlayerInput _inputTwo = null;
        private GameObject _playerOne = null;
        private GameObject _playerTwo = null;

        [SetUp]
        public override void Setup()
        {
            base.Setup();
            SceneManager.LoadScene("CharacterControl_Multiplayer_Sandbox");
            _keyboard = InputSystem.AddDevice<Keyboard>();
        }
        
        private void PlayerInputSetup()
        {
            var players = GameObject.FindObjectsOfType<PlayerInput>();
            _playerOne = players[0].gameObject;
            _inputOne = players[0];
            _inputOne.SwitchCurrentControlScheme("Keyboard&Mouse_Keys", Keyboard.current);

            _playerTwo = players[1].gameObject;
            _inputTwo = players[1];
            _inputTwo.SwitchCurrentControlScheme("Keyboard&Mouse_WASD", Keyboard.current);
        }

        [UnityTest]
        public IEnumerator Test_SandboxMultiplayer_NumberOfPlayers()
        {
            yield return null;
            var players = GameObject.FindObjectsOfType<PlayerInput>();

            Assert.AreEqual(players.Length, 2, "Number of players in scene should be 2");
        }
        
        [UnityTest]
        public IEnumerator Test_BothPressJump_EqualYPosition()
        {
            //Given
            PlayerInputSetup();
            
            //When
            Press(_keyboard.upArrowKey);
            Press(_keyboard.wKey);
            yield return new WaitForSeconds(0.5f);
            
            //Then
            Assert.AreEqual(_playerOne.transform.position.y, _playerTwo.transform.position.y, "Both heights should be equal when jumping");
            yield return null;
        }
        
        [UnityTest]
        public IEnumerator Test_BothRunInwards_DifferentXPosition()
        {
            //Given
            PlayerInputSetup();
            var beforePlayerOne = _playerOne.transform.position.x;
            var beforePlayerTwo = _playerTwo.transform.position.x;
            
            //When
            Press(_keyboard.leftArrowKey);
            Press(_keyboard.dKey);
            yield return new WaitForSeconds(0.5f);
            
            //Then
            Assert.AreNotEqual(beforePlayerOne, _playerOne.transform.position.x, "X position should be different");
            Assert.AreNotEqual(beforePlayerTwo, _playerTwo.transform.position.x, "X position should be different");
            yield return null;
        }
        
        [UnityTest]
        public IEnumerator Test_BothRunOutwards_DifferentXPosition()
        {
            //Given
            PlayerInputSetup();
            var beforePlayerOne = _playerOne.transform.position.x;
            var beforePlayerTwo = _playerTwo.transform.position.x;
            
            //When
            Press(_keyboard.rightArrowKey);
            Press(_keyboard.aKey);
            yield return new WaitForSeconds(0.5f);
            
            //Then
            Assert.AreNotEqual(beforePlayerOne, _playerOne.transform.position.x, "X position should be different");
            Assert.AreNotEqual(beforePlayerTwo, _playerTwo.transform.position.x, "X position should be different");
            yield return null;
        }
    }
}