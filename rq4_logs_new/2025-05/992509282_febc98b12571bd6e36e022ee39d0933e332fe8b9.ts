import NextAuth from "next-auth"
import Strava from "next-auth/providers/strava"

const handler = NextAuth({
    providers: [
        Strava({
            clientId: process.env.STRAVA_CLIENT_ID!,
            clientSecret: process.env.STRAVA_CLIENT_SECRET!,
            authorization: {
                params: {
                    approval_prompt: "auto",
                    scope: "profile:read_all,activity:read,activity:read_all",
                }, 
            },
        }),
    ],
})

export { handler as GET, handler as POST }