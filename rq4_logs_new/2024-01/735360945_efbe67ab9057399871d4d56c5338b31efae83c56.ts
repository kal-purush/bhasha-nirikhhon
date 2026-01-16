import { RANDOM_IMAGE } from "./consts";
import { Project, User } from "./types";

export const user1 : User = {
      id: 1,
      email: "",
      username: "Shayan",
      profile_pic: RANDOM_IMAGE,
}

export const user2 : User = {
      id: 2,
      email: "",
      username: "Arshia",
      profile_pic: RANDOM_IMAGE,
}

export const user3 : User = {
      id: 3,
      email: "",
      username: "Sara",
      profile_pic: RANDOM_IMAGE,
}

export const user4 : User = {
      id: 4,
      email: "",
      username: "Sepehr",
      profile_pic: RANDOM_IMAGE,
}

export const project1 : Project = {
      id: 1,
      name: "Narm 1",
      owner: user1,
      admins: [user1, user2],
      members: [user1, user2, user3],
      background: RANDOM_IMAGE,
      link: ""
}