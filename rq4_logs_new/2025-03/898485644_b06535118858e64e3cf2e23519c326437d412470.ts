import { prisma } from "@/lib/prismaClient";
import NextAuth from "next-auth";
import GitHubProvider from "next-auth/providers/github";
// import GoogleProvider from "next-auth/providers/google";

export const { handlers, signIn, signOut, auth } = NextAuth({
  providers: [
    // GoogleProvider({
    //   clientId: process.env.GOOGLE_CLIENT_ID as string,
    //   clientSecret: process.env.GOOGLE_CLIENT_SECRET as string,
    // }),
    GitHubProvider({
      clientId: process.env.GITHUB_CLIENT_ID as string,
      clientSecret: process.env.GITHUB_CLIENT_SECRET as string,
    }),
  ],
  debug: true,
  secret: process.env.NEXTAUTH_SECRET,
  callbacks: {
    async jwt({ token, account, profile }) {
      if (account) {
        token.provider = account.provider;
        token.id = account.providerAccountId;

        const userId = account.providerAccountId;
        const userName = profile?.name as string;
        const userEmail = profile?.email as string;
        const userImage = profile?.avatar_url || "";

        await prisma.user.upsert({
          where: {
            id: userId,
          },
          update: {
            username: userName,
            email: userEmail,
            iconUrl: userImage,
          },
          create: {
            id: userId,
            username: userName,
            email: userEmail,
            iconUrl: userImage,
          },
        });
      }
      return token;
    },

    async session({ session, token }: any) {
      session.user.provider = token.provider;
      session.user.id = token.id;
      return session;
    },
  },
});