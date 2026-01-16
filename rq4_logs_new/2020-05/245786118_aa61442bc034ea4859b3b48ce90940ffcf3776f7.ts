import jwt from 'jsonwebtoken'
import cookie from 'cookie'

const createJwtCookie = (userId: number) => {
    const secretKey = `-----BEGIN RSA PRIVATE KEY-----\n${process.env.JWT_SECRET_KEY}\n-----END RSA PRIVATE KEY-----`

    const token = jwt.sign({ userId }, secretKey, {
        algorithm: 'RS256',
        expiresIn: '1 days'
    })

    const jwtCookie = cookie.serialize('jwt', token, {
        // local isn't https so check the secure tag
        secure: process.env.NETLIFY_DEV !== 'true',
        httpOnly: true,
        path: '/'
    })

    return jwtCookie
}

export const clearCookie = () =>
    'jwt=deleted; path=/; expires=Thu, 01 Jan 1970 00:00:00 GMT'

export default createJwtCookie