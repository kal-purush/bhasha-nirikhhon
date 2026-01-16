export const sleep = async () =>
  new Promise((resolve) => setTimeout(resolve, Math.random() * 10000));