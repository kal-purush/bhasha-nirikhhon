import passport from "passport";
import type { NextApiRequest, NextApiResponse } from "next";
import nc from "next-connect";
import { localStrategy } from "../../../lib/passport-local";
import { setUserSession } from "../../../lib/auth.session";
import { IUser } from "../../../interfaces";

const handler = nc<NextApiRequest, NextApiResponse>();

const authOptions = { session: false } as passport.AuthenticateOptions;

const authenticate = (req: NextApiRequest, res: NextApiResponse) =>
  new Promise<IUser>((resolve, reject) => {
    passport.authenticate("local", authOptions, (error, loggedData) => {
      if (error) {
        reject(error);
      } else {
        resolve(loggedData);
      }
    })(req, res);
  });

passport.use(localStrategy);

handler.use(passport.initialize()).post(async (req, res) => {
  try {
    const user = await authenticate(req, res);

    await setUserSession(res, user);

    res.status(200).send({ done: true });
  } catch (e) {
    const error = e as Error;
    console.error(error);
    res.status(401).send({ message: error.message });
  }
});

export default handler;