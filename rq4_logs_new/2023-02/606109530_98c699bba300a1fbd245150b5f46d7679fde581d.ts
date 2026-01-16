import getApiData from "../getApiData/getApiData";
import { friendsStructure, friendStructure } from "../types";

let friendsData: friendsStructure = [];

export const getFriendsData = async () => {
  const data = await getApiData();

  for (let i = 0; i < data.friends.length; i++) {
    const friend: friendStructure = {
      name: data.friends[i].name,
      age: data.friends[i].age,
      country: data.friends[i].country,
      id: data.friends[i].id,
    };
    friendsData.push(friend);
  }
  return await friendsData;
};