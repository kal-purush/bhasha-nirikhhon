using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class JoypadSelector : MonoBehaviour {
    public enum Joypad {
        Mouse,
        Joypad0,
        Joypad1,
        Joypad2,
        Joypad3,
    };

    public Joypad joypad;
    WaveController waveController;

    void Awake() {
        waveController = gameObject.GetComponent<WaveController>();
    }

    void Start() {
        WaveInput input = null;
        switch (joypad) {
        case Joypad.Mouse:
            input = gameObject.AddComponent<MouseWaveInput>() as WaveInput;
            break;
        }

        print(waveController);
        print(input);

        waveController.input = input;
    }
}