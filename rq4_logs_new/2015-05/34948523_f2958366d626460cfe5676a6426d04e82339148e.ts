var amp = new Amplifier('最高級アンプ');
var tuner = new Tuner('最高級AM/FMチューナー', amp);
var dvd = new DvdPlayer('最高級DVDプレイヤー', amp);
var cd = new CdPlayer('最高級CDプレイヤー', amp);
var projector = new Projector('最高級プロジェクター', dvd);
var lights = new TheaterLights('シアター用照明');
var theaterScreen = new TheaterScreen('シアタースクリーン');
var popper = new PopcornPopper('ポップコーン製造機');

var homeTheater =
    new HomeTheaterFacade(
        amp,
        tuner,
        dvd,
        cd,
        projector,
        theaterScreen,
        lights,
        popper
        );

homeTheater.watchMovie('レイダース/失われたアーク<聖櫃>')

homeTheater.endMovie();