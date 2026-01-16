using UnityEngine;

public class JoyWaveInput : WaveInput {
  public string axis = "Joypad0";
  float lastValue;
  public override float GetAxis() {
    // Joystick Sensitvity.
    // Add the joystick to the last value making it a second (overall third)
    // order movement control. Then map the linear values to a sin curve.
    var newValue = Mathf.Clamp(Input.GetAxis(axis) * Time.deltaTime * 3 + lastValue * (1 - Time.deltaTime), -1, 1);
    lastValue = newValue;
    return Mathf.Sin(newValue * Mathf.PI / 2) * 250;
  }
}