export const formatDate = (date:string):string => {
    const dates = date.split('-')
    const year = dates[0]
    const month = dates[1]
    const day = dates[2]

    return `${months[parseInt(month)-1]} ${parseInt(day)}, ${year}`
}

const months = [
    'Jan',
    'Feb',
    'Mar',
    'Apr',
    'May',
    'Jun',
    'Jul',
    'Aug',
    'Sep',
    'Oct',
    'Nov',
    'Dec'
]