interface IDateFormat {
  (date?: Date | string | number, mask?: string, utc?: boolean, gmt?: boolean): string;
  masks: { [name: string]: string; };
}

const dateformat: IDateFormat;

export = dateformat;