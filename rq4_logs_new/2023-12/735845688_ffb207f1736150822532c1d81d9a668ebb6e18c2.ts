import axios from "axios";
import { toast } from "react-toastify";

const baseUrl = process.env.NEXT_PUBLIC_BASE_URL;
const AddProject = {
  postAddProject: async (
    token: string | undefined,
    title: string,
    link: string,
    description: string
  ) => {
    console.log({ title, link, description });
    return axios
      .post(
        `${baseUrl}/project`,
        {
          title: title,
          link: link,
          description: description,
        },
        {
          headers: {
            Authorization: `Bearer ${token}`,
          },
        }
      )
      .then((res) => res.data)
      .catch((error) => toast.error(error?.response?.data?.message));
  },
};

export default AddProject;