import { Request, Response } from 'express';
import jsonwebtoken from "jsonwebtoken"
import passport from 'passport';
import { db } from '../../../connection/connection';
import { Strategy } from 'passport-local';
import { compareSync } from 'bcrypt';
import { User } from '../../../schema/user';

const localStrategy = new Strategy(
    { usernameField: 'email' },
    async (email, password, done) => {
        try {
            const usersRef = db.collection('users');
            const snapshot = await usersRef.where('email', '==', email).get();
            if (snapshot.empty) {
                return done(null, false, { message: 'Invalid credentials' });
            }
            const userDoc = snapshot.docs[0];
            const user = userDoc.data();
            const passwordIsValid = compareSync(password, user.password);
            if (!passwordIsValid) {
                return done(null, false, { message: 'Invalid credentials' });
            }
            return done(null, user);
        } catch (error) {
            return done(error);
        }
    }
);
passport.use('localStrategy', localStrategy);

passport.serializeUser((User: User, done) => {
    done(null, User.email);
});

passport.deserializeUser(async (email: string, done) => {
    try {
        const usersRef = db.collection('users');
        const snapshot = await usersRef.where('email', '==', email).get();
        if (snapshot.empty) {
            return done(null, false);
        }
        const userDoc = snapshot.docs[0];
        const user = userDoc.data();
        return done(null, user);
    } catch (error) {
        return done(error);
    }
}); //todo esto despues se hara un middlware de auth de passport

export const loginUser = async (req: Request, res: Response): Promise<void> => {
    passport.authenticate('localStrategy', (err, user) => {
        if (err) {
            return res.status(500).json(err);
        }
        if (!user) {
            return res.status(401).json({ message: 'Invalid credentials' });
        }
        req.logIn(user, (err) => {
            if (err) {
                return res.status(500).json(err);
            }
            const jwt = jsonwebtoken
            const secretKey = 'mySecretKey'; // despues se cambiara por otra secrekey bien elaborada y especial para user
            const token = jwt.sign({ email: user.email }, secretKey);
            return res.json({ token });
        });
    })(req, res);
};