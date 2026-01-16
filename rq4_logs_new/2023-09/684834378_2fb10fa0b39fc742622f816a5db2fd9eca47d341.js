import getRandomInt from "@/helpers/getRandomInt";

export const incrementRandomInt = async ({ commit }) => {
  commit("setIsLoading", true);

  const randomInt = await getRandomInt();
  commit("incrementByRandom20", randomInt);

  commit("setIsLoading", false);
};