import { Request, Response } from 'express'
import axios from 'axios'
import qs from 'qs'
import jwt from 'jsonwebtoken'
import User from '../models/User'

export const googleOAuthCallback = async (
    req: Request,
    res: Response
): Promise<void> => {
    try {
        const code = req.query.code as string

        if (!code) {
            res.status(400).json({ message: 'No code provided' })
            return
        }

        const tokenRes = await axios.post(
            'https://oauth2.googleapis.com/token',
            qs.stringify({
                code,
                client_id: process.env.GOOGLE_CLIENT_ID,
                client_secret: process.env.GOOGLE_CLIENT_SECRET,
                redirect_uri: 'http://localhost:5000/api/auth/google/callback', // <-- fix
                grant_type: 'authorization_code',
            }),
            { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } }
        )

        const { access_token } = tokenRes.data

        const userInfoRes = await axios.get(
            'https://www.googleapis.com/oauth2/v2/userinfo',
            {
                headers: { Authorization: `Bearer ${access_token}` },
            }
        )

        const { email, name } = userInfoRes.data

        let user = await User.findOne({ email })

        if (!user) {
            user = await User.create({
                name,
                email,
                password: '',
                oauth: true,
            })
        }

        const token = jwt.sign({ id: user._id }, process.env.JWT_SECRET!, {
            expiresIn: '1d',
        })

        res.redirect(`http://localhost:3000/oauth-success?token=${token}`)
    } catch (err) {
        console.error('OAuth error:', err)
        res.status(500).json({
            message: 'OAuth failed',
            error: err instanceof Error ? err.message : String(err),
        })
    }
}