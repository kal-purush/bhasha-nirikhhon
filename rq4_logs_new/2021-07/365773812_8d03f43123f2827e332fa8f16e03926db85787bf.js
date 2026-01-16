import bcrypt from "bcrypt";
import client from "../client";

export default {
  Mutation: {
    createAccount: async (_, { name, userName, email, password }) => {
      try {
        const existingUser = await client.user.findFirst({
          where: {
            OR: [{ userName }, { email }],
          },
        });
        if (existingUser) {
          throw new Error("사용자이름 혹은 이메일이 이미 존재합니다.");
        }
        const hashed = await bcrypt.hash(password, 10);
        return client.user.create({
          data: { userName, email, name, password: hashed },
        });
      } catch (error) {
        return error;
      }
    },
    login: async (_, { userName, password }) => {
      const user = await client.user.findFirst({ where: { userName } });
      if (!user) {
        return {
          ok: false,
          error: "사용자를 찾을 수 없습니다.",
        };
      }
      const checkPassword = await bcrypt.compare(password, user.password);
      if (!checkPassword) {
        return {
          ok: false,
          error: "비밀번호가 틀렸습니다.",
        };
      }
    },
  },
};