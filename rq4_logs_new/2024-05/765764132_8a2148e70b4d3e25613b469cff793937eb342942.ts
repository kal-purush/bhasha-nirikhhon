import {
    isCurrentWeek,
    isToday,
    getLocaleDayOfWeek,
    removeSeconds,
    getDate,
} from '@utils/date'

export const transformServerDate = (date: string) => {
    const resolvedDate = new Date(date)

    if (isToday(resolvedDate)) {
        return removeSeconds(resolvedDate.toLocaleTimeString())
    } else if (isCurrentWeek(resolvedDate)) {
        return getLocaleDayOfWeek(resolvedDate)
    }

    return getDate(resolvedDate)
}

export const transformDate = {
    from: {
        server: transformServerDate,
    },
}