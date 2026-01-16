const prod = {
  url: "https://next-devs12.herokuapp.com",
};

const dev = {
  url: "http://localhost:5000",
};

export const config = prod;
// export const config = process.env.NODE_ENV === "development" ? dev : prod;