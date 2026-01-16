interface Time {
  hours: string;
  minuits: string;
}

export interface TimeInfo {
  [key: string]: string | number | Date | Time;
  date: string;
  day: string;
  week: string;
  time: Time;
}

export interface TimeDocs {
  id: string;
  doc: {data:TimeInfo[]};
}