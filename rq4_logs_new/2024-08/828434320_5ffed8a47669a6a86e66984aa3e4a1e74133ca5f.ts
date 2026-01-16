const editAlarmTime = (
    index: number, 
    setHour: React.Dispatch<React.SetStateAction<number>>, 
    setMinutes:React.Dispatch<React.SetStateAction<number>>, 
    alarmTime: 
    {
    hour: number;
    minutes: number;
    }[], 
    setAmOrPm: React.Dispatch<React.SetStateAction<number>>, 
    setEditIndex: React.Dispatch<React.SetStateAction<number>>,
    setModal: (value: React.SetStateAction<boolean>) => void,
    setIsEditMode: (value: React.SetStateAction<boolean>) => void
) => {
  alarmTime.map((item, idx) => {
    if (idx == index) {
      item.hour < 12 ? setAmOrPm(0) : setAmOrPm(1);
      setHour(item.hour % 12);
      setMinutes(item.minutes);
    }
    setEditIndex(index);
    setModal(true);
    setIsEditMode(true);
  });
}

  export default editAlarmTime;