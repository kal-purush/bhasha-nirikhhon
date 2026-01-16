interface challengeBaseInfo {
    id: string;
    name: string; // 알약 이름
    weeklyIntakeFrequency: {
      monday: boolean,
      tuesday: boolean,
      wednesday: boolean,
      thursday: boolean,
      friday: boolean,
      saturday: boolean,
      sunday: boolean
    }; // 주 섭취 횟수 (월 ~ 일)
    notificationTime: {hour:number, minute:number}[]; // 팝업 알림 시간 (19:30 이면 19, 30)
}

export interface pillInfo extends challengeBaseInfo {
    intakeTime: { beforeOrAfterMeal:number, minutes:number}; // 섭취 시간 (식전 1 식후 2, 분 number로)
    dailyIntakePeriod: { breakfast: boolean, lunch: boolean, dinner: boolean  }; // 일 섭취 시기 (아침, 점심, 저녁)
}

export interface habitInfo extends challengeBaseInfo {
    // 다른 필드 없음
}



export default challengeBaseInfo