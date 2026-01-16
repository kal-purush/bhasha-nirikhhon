export const getFormattedDate = (date: string) => {
    let formattedDate = new Date(date);
    return formattedDate.toLocaleDateString('en-US');
};

export const getFormattedTime = (date: string) => {
    const formattedTime = new Date(date);
    let timeString = formattedTime.toLocaleTimeString('en-US');

    const amPm = timeString.substring(timeString.length - 2, timeString.length);
    const hoursAndMinutes = timeString.substring(0, timeString.length - 6);
    timeString = hoursAndMinutes + ' ' + amPm;

    return timeString;
};

export const getTimeLeftToJoin = (timeLimit: string) => {
    const today = new Date();
    const timeLimitDate = new Date(timeLimit);
    const leftTimeToJoin = timeLimitDate.getTime() - today.getTime();
    const differenceInDays = Math.floor(leftTimeToJoin / (1000 * 3600 * 24));
    if (differenceInDays < 1) {
        const differenceInHours = leftTimeToJoin / (1000 * 3600);

        if (differenceInHours < 1) {
            return '<1 hour left to apply';
        }

        return Math.floor(differenceInHours) + ' hours left to apply';
    }
    return differenceInDays + ' days left to apply';
};

export const formatDateForDateInput = (input: number) => {
    let output;
    if (input < 10) {
        output = '0' + input;
    } else {
        output = input;
    }
    return output;
};