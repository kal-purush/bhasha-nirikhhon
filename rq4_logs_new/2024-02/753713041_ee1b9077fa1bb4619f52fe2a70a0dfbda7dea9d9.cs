using System.Collections;
using System.Collections.Generic;
using NUnit.Framework;
using UnityEngine;
using UnityEngine.TestTools;


public class CameraControllerTests
{
    private GameObject cameraObject;
    private CamController camController;
    private MockInputHandler mockInputHandler;


    [SetUp]
    public void SetUp()
    {
        // Create a new GameObject and add the CamController component to it
        cameraObject = new GameObject("Camera");
        camController = cameraObject.AddComponent<CamController>();

        // Initialize the MockInputHandler
        mockInputHandler = new MockInputHandler();

        // Assign the mock input handler to the CamController
        camController.inputHandler = mockInputHandler;
    }


    [TearDown]
    public void TearDown()
    {
        // Cleanup 
        Object.DestroyImmediate(cameraObject);
    }


    [Test]
    public void CameraMovesUpWhenUpArrowPressed()
    {
        // Set input to simulate Up Arrow press
        mockInputHandler.SetKey(KeyCode.UpArrow, true);

        // Get the initial position 
        var initialPosition = cameraObject.transform.position;

        // Trigger the logic
        camController.Movement(); 

        // Check if the camera has moved up (y component increase)
        Assert.Greater(cameraObject.transform.position.y, initialPosition.y, "Camera did not move up as expected.");
    }
    


    [Test]
    public void CameraMovesDownWhenDownArrowPressed()
    {
        // Set input to simulate Down Arrow press
        mockInputHandler.SetKey(KeyCode.DownArrow, true);

        // Get the initial position 
        var initialPosition = cameraObject.transform.position;

        // Trigger the logic
        camController.Movement(); 

        // Check if the camera has moved down
        Assert.AreNotEqual(initialPosition, cameraObject.transform.position);
    }


    [Test]
    public void CameraMovesLeftWhenLeftArrowPressed()
    {
        // Set input to simulate Left Arrow press
        mockInputHandler.SetKey(KeyCode.LeftArrow, true);

        // Get the initial position 
        var initialPosition = cameraObject.transform.position;

        // Trigger the logic
        camController.Movement(); 

        // Check if the camera has moved left
        Assert.AreNotEqual(initialPosition, cameraObject.transform.position);
    }


    [Test] 
    public void CameraMovesRightWhenRightArrowPressed()
    {
        // Set input to simulate Right Arrow press
        mockInputHandler.SetKey(KeyCode.RightArrow, true);

        // Get initial position 
        var initialPosition = cameraObject.transform.position;

        // Trigger the logic
        camController.Movement(); 

        // Check if camera has moved right
        Assert.AreNotEqual(initialPosition, cameraObject.transform.position);
    }


    [Test]
    public void CameraZoomsInWhenReturnPressed()
    {
        // Set input to simulate Return press
        mockInputHandler.SetKey(KeyCode.Return, true);

        // Get initial position 
        var initialPosition = cameraObject.transform.position;

        // Trigger the logic
        camController.ZoomControl(); 

        // Check if camera has zoomed in
        Assert.AreNotEqual(initialPosition, cameraObject.transform.position);
    }


    [Test]
    public void CameraZoomsOutWhenRightShiftPressed()
    {
        // Set input to simulate Right Shift press
        mockInputHandler.SetKey(KeyCode.RightShift, true);

        // Get the initial position 
        var initialPosition = cameraObject.transform.position;

        // Trigger the logic
        camController.ZoomControl(); 

        // Check if camera has zoomed out
        Assert.AreNotEqual(initialPosition, cameraObject.transform.position);
    }


    [Test] // Edge Case
    public void CameraShouldntMoveWhenUpAndDownArrowsPressed()
    {
        // Set input to simulate both Up and Down Arrow keys being pressed
        mockInputHandler.SetKey(KeyCode.UpArrow, true);
        mockInputHandler.SetKey(KeyCode.DownArrow, true);

        // Get initial position
        var initialPosition = cameraObject.transform.position;

        // Trigger the logic
        camController.Movement(); 

        // Check if camera's position has not changed
        Assert.AreEqual(initialPosition, cameraObject.transform.position, "Camera moved when it shouldn't have.");
    }


    [Test]  // Edge Case
    public void CameraShouldntMoveWhenLeftAndRightArrowsPressed()
    {
        // Set input to simulate both Left and Right Arrow keys being pressed
        mockInputHandler.SetKey(KeyCode.LeftArrow, true);
        mockInputHandler.SetKey(KeyCode.RightArrow, true);

        // Get initial position
        var initialPosition = cameraObject.transform.position;

        // Trigger the logic
        camController.Movement(); 

        // Check if camera's position has not changed
        Assert.AreEqual(initialPosition, cameraObject.transform.position, "Camera moved when it shouldn't have.");
    }


    /*[Test]
    public void CameraRotatesRightWhenRightArrowAndSpacePressed()
    {
        // Arrange
        mockInputHandler.SetKey(KeyCode.RightArrow, true);
        mockInputHandler.SetKey(KeyCode.Space, true);
        camController.testDeltaTime = 1 / 60f * 10; // Simulate 10 frames at 60 FPS

        // Get the initial forward direction
        var initialForward = cameraObject.transform.forward;

        // Act
        camController.RotationControl();

        // Assert
        var newForward = cameraObject.transform.forward;
        // Check that the forward vector has changed, indicating a rotation
        Assert.AreNotEqual(initialForward, newForward);
    }

    


    [Test]
    public void CameraRotatesLeftWhenLeftArrowAndSpacePressed()
    {
        // Set input to simulate Left Arrow and Space press
        mockInputHandler.SetKey(KeyCode.LeftArrow, true);
        mockInputHandler.SetKey(KeyCode.Space, true);

        // Get the initial rotation in Euler angles
        var initialRotation = cameraObject.transform.eulerAngles;

        // Trigger the logic
        camController.RotationControl(); 

        // Get the updated rotation in Euler angles
        var newRotation = cameraObject.transform.eulerAngles;

        // Check if the camera has rotated left on the y-axis
        // Using a delta value for comparison to account for floating-point precision issues
        Assert.That(newRotation.y, Is.GreaterThan(initialRotation.y).Or.LessThan(initialRotation.y + 180), "Camera did not rotate right as expected.");
    }


    [Test]
    public void CameraRotatesUpWhenUpArrowAndSpacePressed()
    {
        // Set input to simulate Up Arrow and Space press
        mockInputHandler.SetKey(KeyCode.UpArrow, true);
        mockInputHandler.SetKey(KeyCode.Space, true);

        // Get the initial rotation in Euler angles
        var initialRotation = cameraObject.transform.eulerAngles;

        // Trigger the logic
        camController.RotationControl(); 

        // Get the updated rotation in Euler angles
        var newRotation = cameraObject.transform.eulerAngles;

        // Check if the camera has rotated up on the x-axis
        // Using a delta value for comparison to account for floating-point precision issues
        Assert.That(newRotation.x, Is.GreaterThan(initialRotation.x).Or.LessThan(initialRotation.x + 180), "Camera did not rotate up as expected.");
    }


    [Test]
    public void CameraRotatesDownWhenDownArrowAndSpacePressed()
    {
        // Set input to simulate Down Arrow and Space press
        mockInputHandler.SetKey(KeyCode.DownArrow, true);
        mockInputHandler.SetKey(KeyCode.Space, true);

        // Get the initial rotation in Euler angles
        var initialRotation = cameraObject.transform.eulerAngles;

        // Trigger the logic
        camController.RotationControl(); 

        // Get the updated rotation in Euler angles
        var newRotation = cameraObject.transform.eulerAngles;

        // Check if the camera has rotated down on the x-axis
        // Using a delta value for comparison to account for floating-point precision issues
        Assert.That(newRotation.x, Is.GreaterThan(initialRotation.x).Or.LessThan(initialRotation.x - 180), "Camera did not rotate down as expected.");
    }*/
}

