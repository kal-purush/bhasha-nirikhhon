using UnityEngine;
using System;
using System.Collections.Generic;

public class MouseWaveInput : WaveInput {
  int windowSize = 50;
  float timeMultiple = 1000;
  float lastTime;
  float lastValue;
  Queue<float> buffer;

  bool active;

  float resetWidth = 1;
  float resetLength = 250;

  int[] peakIndices;
  int[] troughIndices;
  float resetWidth0;
  int enqueueCount;
  int bufferLength;

  void Start() {
    lastTime = Time.deltaTime;
    lastValue = 0;
    buffer = new Queue<float>();
  }

  void Reset() {
    buffer = new Queue<float>();
  }

  void Update() {
    if (buffer == null) {
      buffer = new Queue<float>();
    }

    var newValue = Input.GetAxis("Horizontal") + lastValue;
    lastValue = newValue;

    // Figure out how much time has passed, enqueue values until now.
    var lastTimeRound = Mathf.Floor(lastTime * timeMultiple) / timeMultiple;
    var timeRound = Mathf.Floor(Time.time * timeMultiple) / timeMultiple;
    lastTime = Time.time;
    enqueueCount = (int) ((timeRound - lastTimeRound) * timeMultiple);
    for (int i = 0; i < enqueueCount; i++) {
      buffer.Enqueue(newValue);
    }

    if (enqueueCount == 0) {
      return;
    }

    // Find last two peaks
    peakIndices = new int[] {-1, 0};
    float[] peaks = {Mathf.NegativeInfinity, buffer.Peek()};
    {
      int i = 0;
      float last2V = Mathf.NegativeInfinity;
      float lastV = Mathf.NegativeInfinity;
      foreach (float v in buffer) {
        if (lastV > last2V && lastV > v && i - peakIndices[1] > windowSize) {
          peakIndices[0] = peakIndices[1];
          peaks[0] = peaks[1];
          peakIndices[1] = i;
          peaks[1] = v;
        }
        if (lastV != v) {
          last2V = lastV;
          lastV = v;
        }
        i++;
      }
      bufferLength = i;
    }
    // Find last two troughs
    troughIndices = new int[] {-1, 0};
    float[] troughs = {Mathf.Infinity, buffer.Peek()};
    {
      int i = 0;
      float last2V = Mathf.Infinity;
      float lastV = Mathf.Infinity;
      foreach (float v in buffer) {
        if (lastV < last2V && lastV < v && i - troughIndices[1] > windowSize) {
          troughIndices[0] = troughIndices[1];
          troughs[0] = troughs[1];
          troughIndices[1] = i;
          troughs[1] = v;
        }
        if (lastV != v) {
          last2V = lastV;
          lastV = v;
        }
        i++;
      }
    }

    // Drop extra buffer values after second last peak or trough
    var dropCount = Mathf.Min(troughIndices[0], peakIndices[0]);
    for (int i = 0; i < dropCount; i++) {
      buffer.Dequeue();
    }

    // Calculate time between peaks and troughs, this is the raw accel
    var peakDiff = peakIndices[1] - peakIndices[0];
    var troughDiff = troughIndices[1] - troughIndices[0];
    var diff = Mathf.Min(peakDiff, troughDiff);
    var rawAccel = timeMultiple / diff;

    // Calculate the width between the last peek and trough, this is the power
    var width1 = peaks[1] - troughs[1];
    var width0 = peaks[0] - troughs[0];
    var power = Mathf.Abs(width1);

    active = power > 10;

    if (active) {
      int i = 0;
      float lastV = lastValue;
      float max = Mathf.NegativeInfinity;
      float min = Mathf.Infinity;
      foreach (int v in buffer) {
        if (bufferLength - i >= resetLength) {
          lastV = v;
        }
        if (bufferLength - i < resetLength) {
          max = Mathf.Max(max, v - lastV);
          min = Mathf.Min(min, v - lastV);
        }
        i++;
      }
      resetWidth0 = Mathf.Abs(max - min);
      if (Mathf.Abs(max - min) < resetWidth) {
        // lastValue = 0;
        accel = 0;
        // direction = 0;
        buffer.Clear();
      }

      if (!(troughIndices[0] <= 0 && peakIndices[0] <= 0)) {
        // Multiply raw accel and power, this is the accel
        accel = rawAccel * power / 1000;

        // Calculate center between peeks and troughs, this is the direction
        var center1 = peaks[1] / 2 + troughs[1] / 2;

        direction = -1 * center1 / 100;
      }
      else {
        accel = 0;
        // direction = 0;
      }
    }
    else {
      for (int i = 0; i < bufferLength - resetLength; i++) {
        accel = 0;
        // direction = 0;
        buffer.Dequeue();
      }
    }

    direction = lastValue / -100;
  }
}