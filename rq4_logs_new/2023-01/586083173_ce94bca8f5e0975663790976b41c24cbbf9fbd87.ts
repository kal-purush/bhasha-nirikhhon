export const formatDate = (date: Date): string => {
    const monthNames = [
        'January',
        'February',
        'March',
        'April',
        'May',
        'June',
        'July',
        'August',
        'September',
        'October',
        'November',
        'December'
    ];

    const month = monthNames[date.getMonth()];
    const day = date.getDate().toString();

    const hour = date.getHours().toString();
    const minute = date.getMinutes().toString();

    return `${day}. ${month} ${hour}:${minute}`;
};