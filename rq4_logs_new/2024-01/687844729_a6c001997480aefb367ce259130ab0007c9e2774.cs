using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using UnityEngine;

public class InputManager : MonoBehaviour
{
    private Thread _inputThread;

    // Temporary signal indicator. Might be replaced by Windows Message later.
    private bool _isGameRunning;
    private DateTime _lastFrameUpdate;

    [DllImport("user32")]
    static extern short GetKeyState(int nVirtKey);
    
    void Start()
    {
        _isGameRunning = false;
        _lastFrameUpdate = DateTime.Now;
    }

    void Update()
    {
        _lastFrameUpdate = DateTime.Now;
    }

    /// <summary>
    /// Starts the input thread with given keys
    /// </summary>
    /// <param name="keys">A list of keys which should be registered.</param>
    public void StartLoop(List<KeyCode> keys)
    {
        _isGameRunning = true;
        _inputThread = new Thread(InputLoop);
        _inputThread.Start(keys);
    }

    /// <summary>
    /// Stops the input thread.
    /// </summary>
    public void StopLoop()
    {
        _isGameRunning = false;
        _inputThread?.Join();
    }

    /// <summary>
    /// Input Loop Implementation.
    /// </summary>
    /// <param name="keys">A list of keys which should be processed when pressed.</param>
    private void InputLoop(object keys)
    {
        List<KeyCode> registeredKeys = (List<KeyCode>)keys;
        List<bool> keyActiveState = new() { false, false, false, false, false };
        List<int> registeredKeysAsSystem = registeredKeys.ConvertAll(key => KeyMapping.UnityToSystem(key));

        // This game is 4-key + Space game
        if (registeredKeys.Count != 5) return;

        // Starts the loop
        while (_isGameRunning)
        {
            for (int i = 0; i < registeredKeys.Count; i++)
            {
                double timeDifference = (DateTime.Now - _lastFrameUpdate).TotalMilliseconds / 1000;
                bool keyPressed = IsKeyPressed(registeredKeysAsSystem[i]);
                if (keyPressed && !keyActiveState[i])
                {
                    // Will be implemented
                }
                keyActiveState[i] = keyPressed;
            }
            Thread.Sleep(1);
        }
    }

    /// <summary>
    /// Gets if the given key is pressed.
    /// </summary>
    /// <param name="key">Virtual key code(not Unity)</param>
    /// <returns>true if the given key is pressed; false if not</returns>
    private bool IsKeyPressed(int key)
    {
        short result = GetKeyState(key);
        return (result >> 1) != 0;
    }
}