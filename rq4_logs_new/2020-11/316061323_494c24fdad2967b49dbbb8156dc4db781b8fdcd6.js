import Axios from "axios";

const AxiosUserPosts = Axios.create({
  baseURL: "https://react-test-a67f4.firebaseio.com/",
});

AxiosUserPosts.defaults.headers.common["Authorization"] = "POSTS TOKEN";

export default AxiosUserPosts;