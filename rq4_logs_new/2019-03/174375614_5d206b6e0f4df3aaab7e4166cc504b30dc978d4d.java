package com.coolcompany.smarthome.adapter;

import com.coolcompany.smarthome.events.CCSensorEvent;
import com.coolcompany.smarthome.events.EventHandler;
import ru.sbt.mipt.oop.EventProcessor;
import ru.sbt.mipt.oop.sensor.SensorEvent;

public class AdapterCCToBaseEventProcessor implements EventHandler {

  @Override
  public void handleEvent(CCSensorEvent event){
    eventProcessor.processEvent(new AdapterCCtoBaseEventType(event));
  }

  public AdapterCCToBaseEventProcessor(EventProcessor CCEventProcessor) {
    this.eventProcessor = CCEventProcessor;
  }

  private EventProcessor eventProcessor;
}