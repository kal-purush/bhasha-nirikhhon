import { DateTime } from 'luxon';

const dateFormatOptions = {
    weekday: 'long',
    hour12: true,
    hour: 'numeric',
    minute: 'numeric'
}

const timeFormatOptions = {
    hour12: true,
    hour: 'numeric',
    minute: 'numeric'
}

export enum DateTimeDisplay {
    DateTime,
    Time
}

export const formatDate = (date: DateTime, display: DateTimeDisplay) => {
    const formatOptions = display === DateTimeDisplay.DateTime ? dateFormatOptions : timeFormatOptions;

    return date.toLocaleString(formatOptions);
}