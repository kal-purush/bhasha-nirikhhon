module LetterStudio.Filters {

  export class Time {
    date: any;

    filter(date: any) {
      var sec, min, hour;
      if (date.secound < 10){
        sec = '0' + date.secound;
      } else {
        sec = date.secound;
      }
      if (date.minute < 10){
        min = '0' + date.minute;
      } else {
        min = date.minute;
      }
      if (date.hour < 10){
        hour = '0' + date.hour;
      } else {
        hour = date.hour;
      }

      return hour + ':' + min + ':' + sec;
    }
  }
}