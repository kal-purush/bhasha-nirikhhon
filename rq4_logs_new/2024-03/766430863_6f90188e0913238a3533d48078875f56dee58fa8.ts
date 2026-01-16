export const getAxiosErrorMessage = (error: unknown): string => {
  console.log("🙋‍♀️", error);
  if ((error as any).response) {
    const message = (error as any)?.response?.data?.message;

    return message;
  } else if ((error as any).request) {
    // The request was made but no response was received
    // `error.request` is an instance of XMLHttpRequest in the browser and an instance of
    // http.ClientRequest in node.js
    return "Request failed. Please retry";
  } else {
    const message = (error as any)?.message;

    return message;
  }
};