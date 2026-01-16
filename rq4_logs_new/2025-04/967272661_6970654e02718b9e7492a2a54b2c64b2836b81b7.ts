const AM_START_TIME = 0;
const PM_START_TIME = 12 * 60;
const AM_END_TIME = 11 * 60 + 30;
const PM_END_TIME = 23 * 60 + 30;
const INTERVAL = 30;

const generateTime = () => {
  const formatTime = (hour: number, minute: number) => {
    const paddedHour = hour.toString().padStart(2, '0');
    const paddedMinute = minute.toString().padStart(2, '0');
    return `${paddedHour}:${paddedMinute}`;
  };

  const am = [];
  const pm = [];

  for (let min = AM_START_TIME; min <= AM_END_TIME; min += INTERVAL) {
    const hour = Math.floor(min / 60);
    const minute = min % 60;
    am.push(formatTime(hour, minute));
  }

  for (let min = PM_START_TIME; min <= PM_END_TIME; min += INTERVAL) {
    const hour = Math.floor(min / 60);
    const minute = min % 60;
    pm.push(formatTime(hour, minute));
  }

  return { am, pm };
};

export default generateTime;