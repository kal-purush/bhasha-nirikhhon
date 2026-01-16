import { prisma } from '../../../../prisma/prismaClient';
import { UserHandlers } from 'env';
import { verify } from 'jsonwebtoken';
import bcrypt from 'bcrypt';

const editPassword: UserHandlers['editePassword'] = async (req, res, next) => {
  try {
    const jwtPayload = verify(req.cookies.token, process.env.SECRET as string);
    if (typeof jwtPayload === 'string') {
      return res
        .status(401)
        .json({ message: 'You need to login', type: 'LOGIN_ERROR' });
    }
    const { oldPassword, password } = req.body;
    console.log(jwtPayload.userId);

    const oldUser = await prisma.user.findUnique({
      where: {
        id: jwtPayload.userId,
      },
      select: {
        password: true,
      },
    });

    const comparePasswords = await bcrypt.compare(
      oldPassword,
      oldUser!.password
    );

    if (!comparePasswords) {
      res.status(422);

      throw new Error('Old password is incorrect.');
    }

    const newUser = await prisma.user.update({
      where: {
        id: jwtPayload.userId,
      },
      data: {
        password: await bcrypt.hash(password, 10),
      },
    });

    res.status(200).json(newUser);
  } catch (e) {
    next(e);
  }
};

export default editPassword;