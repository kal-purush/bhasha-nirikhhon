type NumericString = `${number}`;
type teamId = number | NumericString;
type taskId = number | NumericString;

const PATHS = {
  HOME: '/',
  LOGIN: '/login',
  SIGNUP: '/signup',
  SIGNUP_KAKAO: '/oauth/signup/kakao',
  MYPAGE: '/mypage',
  ADDTEAM: '/addteam',
  MYHISTORY: '/myhistory',
  BOARDS: '/boards',

  getTeamPath: (teamId: teamId) => `${teamId}`,
  getTeamTaskListPath: (teamId: teamId) => `${teamId}/tasklist`,
  getTeamTaskDetailPath: (teamId: teamId, taskId: taskId) => `${teamId}/${taskId}`,
};

export default PATHS;