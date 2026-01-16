import {
    atom
} from "recoil"

interface DateType {
    year?:number;
    month?:number;
    date?:number;
    day?:number;
}

// const calendarDate = atom<DateType>({
//     key:"calendarDate",
//     default:{
//         year:0,
//         month:0,
//         date:0,
//         day:0
//     }
// })

const calendarYear = atom<number>({
    key:"calendarYear",
    default:0
})
const calendarMonth = atom<number>({
    key:"calednarMonth",
    default:0
})
const calendarDate = atom<number>({
    key:"calendarDate",
    default:0
})
const calendarDay = atom<number>({
    key:"calednarDay",
    default:0
})

export {calendarYear,calendarMonth,calendarDate,calendarDay}