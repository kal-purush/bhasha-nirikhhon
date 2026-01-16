//note descArg is type of info needed like weekday, month, year, hour, minute, second or timeZoneName
//typeArg describes what type of info is needed long, short, numeric
export function convertUTC(num, descArg, typeArg){
    const milliseconds = num * 1000;
    const dateObj = new Date(milliseconds);

    switch(descArg) {
        case 'weekday':
            return dateObj.toLocaleString("en-US", {weekday: typeArg});
        case 'hour':
            return dateObj.toLocaleString("en-US", {hour: typeArg});
        case 'time':
            return dateObj.toLocaleTimeString("en-US", {timeStyle: typeArg});
        default:
            return dateObj.toLocaleString("en-US", {weekday: typeArg})
    }
};