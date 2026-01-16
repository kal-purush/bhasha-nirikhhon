import { ChangeEvent } from "react";
import pillInfo from "../../../../../store/pillInfo";

export const handleBeforeOrAfterMeal = <T extends pillInfo>(
  value: number, 
  setter: (value: React.SetStateAction<Omit<T, "id" | "notificationTime">>) => void, 
  newPill: Omit<T, "id" | "notificationTime">
  ): void => {
    setter({...(newPill as T), intakeTime:{...(newPill as T).intakeTime, beforeOrAfterMeal: value}});
    // newPill.intakeTime.beforeOrAfterMeal = value;
  };

  // 이벤트 핸들러: 식사 전후 복용 시간
export const handleMealMinute = <T extends pillInfo>(
  inputElement: HTMLInputElement,
  setter: (value: React.SetStateAction<T>) => void, 
  newPill: Omit<T, "id" | "notificationTime">): void => {
    const filteredValue = inputElement.value.replace(/[^0-9]/g, "");
    inputElement.value = filteredValue;

    setter({...newPill as T,intakeTime:{...newPill.intakeTime, minutes:filteredValue as unknown as number}});
};

export const handleEatingTiming = <T extends pillInfo>(
    e: ChangeEvent<HTMLInputElement>,
    value: string,
    setter: (value: React.SetStateAction<T>) => void, 
    newPill: Omit<T, "id" | "notificationTime">
  ): void => {

    setter({...newPill as T, dailyIntakePeriod: {
      ...newPill.dailyIntakePeriod,
      [value]: !(newPill.dailyIntakePeriod as any)[value],
    }});

};
