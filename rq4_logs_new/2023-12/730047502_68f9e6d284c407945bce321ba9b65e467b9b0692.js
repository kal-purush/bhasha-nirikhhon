import dbConnect from "@/utils/mongodb";
import Credentials from "next-auth/providers/credentials";
import NextAuth from "next-auth/next";
import bcrypt from "bcryptjs";

export default NextAuth({
    providers: [
        Credentials({
            name: 'credentials',
            credentials: {
                email: { label: 'email', type: 'text' },
                password: {  label: 'password', type: 'password' },
            },
            authorize: async (credentials, req) => {
                const { db } = await dbConnect();
                const user = await db.collection('users').findOne({ email: credentials.email });
        
                if (user && bcrypt.compareSync(password, user.passwordHash)) {
                    return Promise.resolve(user);
                } else {
                    return Promise.resolve(null);
                }
            }
        })
    ],
    database: process.env.MONGO_URL
});