import apiClient from './twurpleClient.js';

const getUserIdByName = async (userName) => {
  const user = await apiClient.users.getUserByName(userName);
  if (!user) {
    return false;
  }
  return user.id;
};

const isStreamLive = async (userName) => {
  const user = await apiClient.users.getUserByName(userName);
  if (!user) {
    return false;
  }
  return (await user.getStream()) !== null;
};

isStreamLive('bronifty').then((res) => console.log(res));
getUserIdByName('bronifty').then((res) => console.log(res));