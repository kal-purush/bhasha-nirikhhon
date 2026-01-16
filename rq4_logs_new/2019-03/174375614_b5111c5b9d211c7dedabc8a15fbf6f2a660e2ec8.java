package ru.sbt.mipt.oop;

import ru.sbt.mipt.oop.sensor.SensorEvent;

import java.util.ArrayList;
import java.util.Collection;

public class EnventHandler {
  EnventHandler() {
    eventProcesses = new ArrayList<>();
  }

  public void addEnvent(EnventHome newEnvent) {
    eventProcesses.add(newEnvent);
  }

  public void processEnvent(SmartHome smartHome, SensorEvent event) {
    for (EnventHome eventProcessor : eventProcesses) {
      eventProcessor.processEnvent(smartHome, event);
    }
  }

  private Collection<EnventHome> eventProcesses;
}