import { customHandler } from '../../../utils/server/commonHandler';
import { verifyJWT } from "../../../utils/encrypt";
import LoginToken from "../../../models/LoginToken";

export interface loginQuery {
    id: string,
    password: string,
    fingerprint: string,
    persistent: boolean
}

const handler = customHandler()
    .get(
        async (req, res) => {
            const cookies = [`access_token=;Max-Age=-1;Path=/;HttpOnly;Secure;SameSite=Strict`,
                `refresh_token=;Max-Age=-1;Path=/;HttpOnly;Secure;SameSite=Strict`]
            res.setHeader('Set-Cookie', cookies)
            const { refresh_token } = req.cookies
            if (refresh_token) {
                const { jti } = await verifyJWT(refresh_token)
                await LoginToken.deleteOne({ jti })
            }
            res.redirect("/")
        }
    )

export default handler