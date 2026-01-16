package headFirst.facade;

public class HomeTheaterTestDrive {
  public static void main(String[] args) {
    Tuner tuner = new Tuner();
    DvdPlayer dvd = new DvdPlayer();
    CdPlayer cd = new CdPlayer();
    Projector projector = new Projector();
    Screen screen = new Screen();
    TheaterLights lights = new TheaterLights();
    Popper popper = new Popper();
    Amplifier amp = new Amplifier(tuner, dvd, cd);

    HomeTheaterFacade homeTheater = new HomeTheaterFacade(
      amp,
      tuner,
      dvd,
      screen,
      popper,
      cd,
      lights,
      projector
    );

    homeTheater.watchMovie("Raiders of the Lost Ark");
    homeTheater.endMovie();
  }
}