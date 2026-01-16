type WeekDate=() => { temp: { min: number; max: number; }; weather: { icon: string; }[]; }[]

export const useWeekDate = () => {

  const weekDates:WeekDate = () => {
    let array = [];
    for(let i = 0; i < 8; i ++) {
      array.push({temp: { min: 0, max: 0 }, weather: [{ icon: "01d" }] })
    }
    return array;
  }

  return{weekDates}
}
