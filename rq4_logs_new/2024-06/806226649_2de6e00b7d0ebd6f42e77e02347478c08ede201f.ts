export class DateUtil {

  private constructor() {}

  private static dateRegex = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2}(?:\.\d*)?)$/;

  private static utcDateRegex = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2}(?:\.\d*)?)Z$/;

  public static convertDates(obj: {[key: string]: any}) {
    if (!obj || !(typeof obj === 'object')) {
      return;
    }

    if (Array.isArray(obj)) {
      for (const item of obj) {
        this.convertDates(item);
      }
    }

    for (const key of Object.keys(obj)) {
      const value = obj[key];

      if (Array.isArray(value)) {
        for (const item of value) {
          this.convertDates(item);
        }
      }

      if (typeof value === 'object') {
        this.convertDates(value);
      }

      if (typeof value === 'string' &&
        (this.dateRegex.test(value) || this.utcDateRegex.test(value))) {
        obj[key] = new Date(value);
      }
    }
  }
}