package ru.sbt.mipt.oop;


import ru.sbt.mipt.oop.randomenvents.RandomChoosingStratagy;
import ru.sbt.mipt.oop.sensor.SensorEvent;
import ru.sbt.mipt.oop.sensor.SensorEventType;

class ProduceNewEnvent {
  SensorEvent getNextSensorEvent() {
    RandomChoosingStratagy rand = new RandomChoosingStratagy();
    // pretend like we're getting the events from physical world, but here we're going to just generate some random events
    if (Math.random() < 0.05) return null; // null means end of event stream
    SensorEventType sensorEventType = SensorEventType.values()[(int) (4 * Math.random())];
    String objectId = "" + rand.Randomizer();
    return new SensorEvent(sensorEventType, objectId);
  }
}