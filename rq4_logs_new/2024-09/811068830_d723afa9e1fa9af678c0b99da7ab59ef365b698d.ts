// Formats a Date object into a short month and numeric day.
export const formatDateShort = (date: Date) => 
    `${date.toLocaleDateString("en", { month: "short" })} ${date.getDate()}`;

// Formats a Date object into a long month name in lowercase.
export const formatDateLong = (date: Date) => 
    date.toLocaleDateString("en", { month: "long" }).toLowerCase();

// Formats a Date object into a long month name and year.
export const formatMonthYear = (date: Date) => 
    `${date.toLocaleDateString("en", { month: "long" })} ${date.getFullYear()}`;

// Formats a Date object to return the first letter of the short weekday name.
export const formatWeekdayShort = (date: Date) => 
    date.toLocaleDateString("en", { weekday: "short" })[0];