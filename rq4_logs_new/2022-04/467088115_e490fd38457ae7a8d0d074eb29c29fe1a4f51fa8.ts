import { SetterOrUpdater } from "recoil"

  // // 앞뒤 버튼만이 calYear, calMonth를 변경할 수 있다
const useMoveCalendarMonth = ({
    changeMonth,
    calYear,
    calMonth,
    setCalYear,
    setCalMonth,
}:{
    changeMonth:number,
    calYear:number,
    calMonth:number,
    setCalYear:SetterOrUpdater<number>,
    setCalMonth:SetterOrUpdater<number>,
}) => {
    const tempDate = new Date(calYear,calMonth + changeMonth)
    console.log(tempDate)
    setCalYear(tempDate.getFullYear())
    setCalMonth(tempDate.getMonth())
}

export default useMoveCalendarMonth