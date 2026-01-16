package ru.sbt.mipt.oop;

import ru.sbt.mipt.oop.objectshome.subjects.alarm.Code;
import ru.sbt.mipt.oop.objectshome.subjects.alarm.AlarmState;
import ru.sbt.mipt.oop.objectshome.subjects.alarm.statesalarm.DeactiveAlarmState;

public class Alarm {
  private AlarmState state;
  private Code code;

  public Alarm(){
    this.state = new DeactiveAlarmState(this);
  }

  public AlarmState getState(){
    return state;
  }

  public void setCode(String newCode){
    code.setCode(newCode);
  }

  public void changeState(AlarmState newState){
    this.state = newState;
  }

  public void activateAlarm(Code code){
    state.activateAlarm(code);
  }

  public void deactivateAlarm(Code code){
    state.deactivateAlarm(code);
  }

  public void turnOnAlert() {}
}