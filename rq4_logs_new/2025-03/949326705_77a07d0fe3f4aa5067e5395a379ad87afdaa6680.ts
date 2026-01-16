import NextAuth from "next-auth"
import CredentialsProvider from "next-auth/providers/credentials"
import EmailProvider from "next-auth/providers/email"
import { MongoDBAdapter } from "@auth/mongodb-adapter"
import clientPromise from "@/lib/db/mongodb"
import dbConnect from "@/lib/db/connection"
import { UserModel, TOTPSessionModel } from "@/lib/db/models"
import { generateEncryptionKey, hashPassword } from "@/lib/encryption"
import { sendTOTPEmail } from "@/lib/email-service"
import { generateTOTP } from "@/lib/encryption"

export const authOptions = {
  adapter: MongoDBAdapter(clientPromise),
  providers: [
    EmailProvider({
      server: {
        host: process.env.EMAIL_HOST,
        port: Number(process.env.EMAIL_PORT),
        auth: {
          user: process.env.EMAIL_USER,
          pass: process.env.EMAIL_PASSWORD,
        },
        secure: process.env.EMAIL_SECURE === "true",
      },
      from: process.env.EMAIL_FROM,
      // Custom sendVerificationRequest function to use our TOTP system
      async sendVerificationRequest({ identifier: email, url }) {
        await dbConnect()

        // Generate TOTP code
        const code = generateTOTP()
        const expiresAt = new Date()
        expiresAt.setMinutes(expiresAt.getMinutes() + 10) // Expires in 10 minutes

        // Save TOTP session
        await TOTPSessionModel.create({
          email,
          code,
          expiresAt,
        })

        // Send email with TOTP code
        await sendTOTPEmail(email, code)
      },
    }),
    CredentialsProvider({
      name: "TOTP",
      credentials: {
        email: { label: "Email", type: "email" },
        code: { label: "Verification Code", type: "text" },
      },
      async authorize(credentials) {
        if (!credentials?.email || !credentials?.code) {
          return null
        }

        await dbConnect()

        // Find TOTP session
        const session = await TOTPSessionModel.findOne({
          email: credentials.email,
          code: credentials.code,
          expiresAt: { $gt: new Date() },
        })

        if (!session) {
          return null
        }

        // Find or create user
        let user = await UserModel.findOne({ email: credentials.email })
        if (!user) {
          user = await UserModel.create({
            email: credentials.email,
            createdAt: new Date(),
            updatedAt: new Date(),
          })
        }

        // Delete used TOTP session
        await TOTPSessionModel.deleteOne({ _id: session._id })

        // Generate encryption key for the user's passwords
        const encryptionKey = generateEncryptionKey()

        return {
          id: user._id.toString(),
          email: user.email,
          encryptionKey: encryptionKey,
          hasPassword: !!user.passwordHash,
        }
      },
    }),
    CredentialsProvider({
      id: "password",
      name: "Password",
      credentials: {
        email: { label: "Email", type: "email" },
        password: { label: "Password", type: "password" },
      },
      async authorize(credentials) {
        if (!credentials?.email || !credentials?.password) {
          return null
        }

        await dbConnect()

        // Find user
        const user = await UserModel.findOne({ email: credentials.email })
        if (!user || !user.passwordHash) {
          return null
        }

        // Verify password
        const hashedPassword = hashPassword(credentials.password)
        if (hashedPassword !== user.passwordHash) {
          return null
        }

        // Generate encryption key for the user's passwords
        const encryptionKey = generateEncryptionKey()

        return {
          id: user._id.toString(),
          email: user.email,
          encryptionKey: encryptionKey,
          hasPassword: true,
        }
      },
    }),
  ],
  session: {
    strategy: "jwt",
    maxAge: 7 * 24 * 60 * 60, // 7 days
  },
  callbacks: {
    async jwt({ token, user }) {
      // Pass the encryption key from the user object to the token
      if (user) {
        token.encryptionKey = user.encryptionKey
        token.hasPassword = user.hasPassword
      }
      return token
    },
    async session({ session, token }) {
      // Pass the encryption key from the token to the session
      if (token && session.user) {
        session.user.id = token.sub
        session.user.encryptionKey = token.encryptionKey
        session.user.hasPassword = token.hasPassword
      }
      return session
    },
  },
  pages: {
    signIn: "/login",
    verifyRequest: "/verify",
    error: "/login",
  },
}

const handler = NextAuth(authOptions)

export { handler as GET, handler as POST }
