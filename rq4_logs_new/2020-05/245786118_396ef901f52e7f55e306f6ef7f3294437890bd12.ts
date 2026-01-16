import jwt from 'jsonwebtoken'
import cookie from 'cookie'

import publicKey from './publicKey'

const authenticatedWrapper = (toBeCheckedCookie: any) => {
    const cookies = toBeCheckedCookie && cookie.parse(toBeCheckedCookie)
    if (!cookies || !cookies.jwt) {
        const response = {
            statusCode: 401,
            body: JSON.stringify({
                message:
                    'There is no jwt cookie, so the request is unauthorized'
            })
        }

        return response
    }

    try {
        const payload = jwt.verify(cookies.jwt, publicKey) as Record<
            string,
            string | number
        >

        const response = { statusCode: 200, body: payload.userId }

        return response
    } catch (err) {
        const response = {
            statusCode: 401,
            body: JSON.stringify({ message: err.message })
        }

        return response
    }
}

export default authenticatedWrapper