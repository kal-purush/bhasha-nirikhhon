import CredentialsProvider from "next-auth/providers/credentials";
import { PrismaAdapter } from "@next-auth/prisma-adapter";
import { PrismaCli } from "../../../../prismaCli/prismaClient";
import { NextAuthOptions } from "next-auth";
interface Credentials {
  email?: string;
  username?: string;
}
import bcrypt from "bcryptjs";

export const authConfig: NextAuthOptions = {
  adapter: PrismaAdapter(PrismaCli),
  providers: [
    CredentialsProvider({
      credentials: {},

      //@ts-ignore
      async authorize(credentials: User, req) {
        let user;

        user = await PrismaCli.user.findFirst({
          //@ts-ignore
          where: {
            OR: [
              { email: credentials.email.toLowerCase() },
              { username: credentials.email.toLowerCase() },
            ],
          },
        });
        if (!user) {
          throw new Error("No user found");
        }

        let hashPass;
        //@ts-ignore
        hashPass = await bcrypt.compare(credentials.password, user.password);

        if (!hashPass) {
          throw new Error("Invalid Password");
        }

        return user;
      },
    }),
  ],
  callbacks: {
    async jwt({ token, user }) {
      if (user) {
        //@ts-ignore
        (token.id = user?.id), (token.name = user?.username);
      }
      return token;
    },

    async session({ session, token }) {
      //@ts-ignore
      session.user.id = token?.id;
      //@ts-ignore
      session.user.name = token?.name;

      return session;
    },
  },

  session: { strategy: "jwt" },
  secret: "1",
};