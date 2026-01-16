import axios from "axios";

export default axios.create({
  baseURL: "https://api.yelp.com/v3/businesses",
  headers: {
    Authorization:
      "Bearer 8MAdOKtKYy0R6R-gRPok1a71BmDnHwV3080jr281c9yVXduf0jF7RLQQlq4mLka21lpWSC2t3-HrMJh96hZJMlMvBGnOis8jB1J1TgZJyF036fRpNhDQ30zUjI0GX3Yx",
  },
});