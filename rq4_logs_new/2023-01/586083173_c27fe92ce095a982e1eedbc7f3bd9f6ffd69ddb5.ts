import moment from 'moment';

export const getDay = (date: string): string => {
    const today = moment().toISOString();

    const yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);

    const tomorrow = new Date(today);
    tomorrow.setDate(tomorrow.getDate() + 1);

    const tomorrowString = tomorrow.toISOString().substring(0, 10);
    const yesterdayString = yesterday.toISOString().substring(0, 10);

    if (today.includes(date)) {
        return 'Today';
    }
    if (tomorrowString.includes(date)) {
        return 'Tomorrow';
    }
    if (yesterdayString.includes(date)) {
        return 'Yesterday';
    }
    return date;
};