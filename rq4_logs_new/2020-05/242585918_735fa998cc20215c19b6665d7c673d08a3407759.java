package headFirst.command;

public abstract class CeilingFanCommand {
  int prevSpeed;
  CeilingFan ceilingFan;

  CeilingFanCommand(CeilingFan ceilingFan) {
    this.ceilingFan = ceilingFan;
  }

  public void undo() {
    if(prevSpeed == ceilingFan.HIGH) {
      ceilingFan.high();
    } else if (prevSpeed == ceilingFan.MEDIUM) {
      ceilingFan.medium();
    } else if (prevSpeed == ceilingFan.LOW) {
      ceilingFan.low();
    } else if (prevSpeed == ceilingFan.OFF) {
      ceilingFan.off();
    }
  }
}