import { headers } from 'next/headers'
import jwt from "jsonwebtoken";
import { ClerkExpressRequireAuth } from '@clerk/clerk-sdk-node';


//import { requireAuth } from '@clerk/nextjs';

const authorizedParties = ['http://localhost:3000', 'https://example.com']

// from clerk dashboard -> settings -> api -> public key
const CLERK_PEM_PUBLIC_KEY = `
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA/uhhuTmLsUW1lESDuADc
ZMaqGCN0z1qttjzLpY1TbDNhz1h6Xdj8nYNgu95ypMywKJeLfxax4rPKmo/Hi45U
uVX89QsXZ39hI83OyOgt76BYuMBoqMysuQ7awmq4JMn8vT4nTdiB5uYMaNkcNFnX
QffzITdnNDXeQdNRTn+ix9lLhsFlcm45ImgIh3EhodbWiq88wQRuS+l8Eg1e+gEd
TMR4GC01JPYiotcVV5wuTwlo5W55r71dtGsngWVtz2R6YF3K+haC4WQZ7THjC1A4
W3KZ9wbEtNWzfnMKUc+RVpyvYOcsIP7XltMnPTXKu2XJ3kzqAQ6XfI/lR4ctRPQ1
uQIDAQAB
-----END PUBLIC KEY-----
`

export async function POST(request: Request) {


    const authorization = headers().get('authorization') || ''
    const [type, token] = authorization.split(' ')

    const decoded = jwt.verify(token, CLERK_PEM_PUBLIC_KEY)
    console.log('Decoded:', decoded)

    console.log('Authorization:', token)

    const userId = (typeof decoded === 'string' ? decoded : decoded.UserID) as string
    return new Response(`Hello, user id: ${userId}`, {
        status: 200,
    })
}