import config from './config';

const yPos = () => window.pageYOffset/window.innerHeight;

export default {
  pos: el => {
    const { baseHeight, elementInterval } = config.height;
    const { baseHeightMult, elementIntervalMult } = el;
    return baseHeight * baseHeightMult + elementInterval * elementIntervalMult;
  },
  // opacity = f(yPos) = -(yPos - k)((yPos - k) - 2) // dimana opacity = 1 (opacityMax) terjadi pada yPos = (k + k + 2)/2 = k + 1.
  // Untuk mencapai opacityMax pada k, maka opacity = -(yPos - (k - 1))(yPos - (k - 1) - 2).
  // agar opacityMax tercapai lebih cepat, maka f(yPos) dibagi dengan bilangan positif < 1. 
  opacity: maxOpacityPos => -1*(yPos() - (maxOpacityPos - 1))*(yPos() - (maxOpacityPos - 1) - 2)/0.7,
  // volume = f(yPos)
  // 0 <= yPos <= maxVolumePos  -> Linear dari 0 sampe maxVolumePos
  // yPos > maxVolumePos        -> dari maxVolume turun ke minVolume di minVolumePos, kemudian naik lagi secara linear (gunakan fungsi absolut)
  volume: (maxVolumePos, minVolumePos) => {
    const { maxVolume, minVolume } = config.volume;
    let volume = yPos() <= maxVolumePos
    ? yPos()/maxVolumePos
    : Math.abs((yPos() - minVolumePos)*(maxVolume - minVolume)/(maxVolumePos - minVolumePos)) + minVolume;

    volume = volume < 0 ? 0 : volume > maxVolume ? maxVolume : volume;
    return volume;
  }
}