import axios from "axios";

export const user = {
  getAll: () =>
    axios.get("http://localhost:5000/api/v1/users").then((res) => res.data),
  getOne: (id: string) =>
    axios
      .get(`http://localhost:5000/api/v1/users/${id}`)
      .then((res) => res.data),

  delete: ({ id }: { id: string }) =>
    axios
      .delete(`http://localhost:5000/api/v1/users/${id}`)
      .then((res) => res.data),
  create: ({ user }: { user: User }) => {
    console.log({ user });

    return axios
      .post("http://localhost:5000/api/v1/users", user)
      .then((res) => res.data);
  },
  update: ({ user, id }: { user: User; id?: string }) => {
    console.log({ user, id });
    if (!id) throw new Error("Id can't be undefined");
    return axios
      .put(`http://localhost:5000/api/v1/users/${id}`, user)
      .then((res) => res.data);
  },
};