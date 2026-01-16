package headFirst.facade;

public class Amplifier {
  Tuner tuner;
  DvdPlayer dvdPlayer;
  CdPlayer cdPlayer;

  public Amplifier(Tuner tuner, DvdPlayer dvdPlayer, CdPlayer cdPlayer) {
    this.tuner = tuner;
    this.dvdPlayer = dvdPlayer;
    this.cdPlayer = cdPlayer;
  }

  public void on() {
    System.out.println("");
  }

  public void off() {
    System.out.println("");
  }

  public void setCd() {
    System.out.println("");
  }

  public void setDvd() {
    System.out.println("");
  }

  public void setStereoSound() {
    System.out.println("");
  }

  public void setSurroundSound() {
    System.out.println("");
  }

  public void setTuner() {
    System.out.println("");
  }

  public void setVolume() {
    System.out.println("");
  }

  public String toString() {
    return "";
  }
}