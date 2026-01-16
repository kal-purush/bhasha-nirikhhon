import type { NextAuthOptions } from "next-auth";

// Providers
import CredentialsProvider from "next-auth/providers/credentials";

export const options: NextAuthOptions = {
  providers: [
    CredentialsProvider({
      name: "Credentials",
      credentials: {
        username: { label: "Username", type: "text" },
        password: { label: "Password", type: "password" },
      },
      async authorize(credentials) {
        const user = {
          id: 1,
          username: "test",
          email: "test@test-mail.com",
          password: "test",
        };

        if (
          user.username === credentials.username &&
          user.password === credentials.password
        ) {
          return user;
        } else {
          return null;
        }
      },
    }),
  ],
};