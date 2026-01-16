import CredentialsProvider from "next-auth/providers/credentials";
import { NextAuthOptions } from "next-auth";
import NextAuth from "next-auth/next";

export const authConfig: NextAuthOptions = {
  providers: [
    CredentialsProvider({
      name: "Email",
      credentials: {
        email: {
          label: "Email",
          type: "email",
          placeholder: "alex.example.com",
        },
        password: { label: "Password", type: "password" },
      },
      async authorize(credentials) {
        interface DB_User {
          id: string;
          name: string;
          email: string;
          password: string;
        }
        const dbUsers: DB_User[] = [
          {
            id: "1",
            name: "James",
            email: "james@example.com",
            password: "password1",
          },
          {
            id: "2",
            name: "Lakshit",
            email: "lakshit@mail.com",
            password: "123123",
          },

        ];

        if (!credentials || !credentials.email || !credentials.password)
          return null;
        const user = dbUsers.find((user) => user.email === credentials.email);

        if (user && user.password === credentials.password) {
          return user; 
        } else {
          return null;
        }
      },
    }),
  ],
};

export default NextAuth(authConfig);