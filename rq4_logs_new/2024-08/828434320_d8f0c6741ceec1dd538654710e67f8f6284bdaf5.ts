export type habbitType = {
  challengeName: string;
  achievementStatus: boolean;
};

export type supplementType = {
  challengeName: string;
  breakfastSuccess: boolean;
  lunchSuccess: boolean;
  dinnerSuccess: boolean;
  breakfastRequired: boolean;
  lunchRequired: boolean;
  dinnerRequired: boolean;
  success: boolean;
};

export type challanegesType = {
  habit: habbitType[];
  supplement: supplementType[];
  achievementRate: number;
  date: string;
};

export type calanderResponseDataType = {
  code: number;
  status: number;
  message: string;
  timestamp: string;
  result: {
    challengeResponses: challanegesType[];
    totalAchievementRate: number;
  };
};

export type dataForCalander = {
  challengeResponses: challanegesType[];
  totalAchievementRate: number;
};