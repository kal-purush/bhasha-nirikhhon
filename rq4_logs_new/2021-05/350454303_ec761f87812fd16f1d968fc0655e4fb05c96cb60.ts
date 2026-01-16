import { Request, Response } from 'express';

import { UserTheme } from '../models/UserTheme';

export async function createUserTheme(request: Request, response: Response): Promise<void> {
  try {
    await UserTheme.findOne({
      where: {
        user: request.body.user,
      },
    }).then(async (user) => {
      if (!user) {
        await UserTheme.create(request.body)
          .then(() => {
            response.status(201).json({ status: 'Ok' });
          })
          .catch((ex) => {
            response.status(500).json(ex);
          });
      } else {
        UserTheme.update(
          {
            theme: request.body.theme,
          },
          {
            where: {
              user: request.body.user,
            },
          }
        )
          .then(() => {
            response.status(201).json({ status: 'Ok' });
          })
          .catch((ex) => {
            response.status(500).json(ex);
          });
      }
    });
  } catch (ex) {
    response.status(500).json(ex);
  }
}

export async function getUserTheme(request: Request, response: Response): Promise<void> {
  try {
    await UserTheme.findOne({
      where: {
        user: request.query.user,
      },
    })
      .then((result) => {
        if (result) {
          response.status(200).send(result.theme);
        } else {
          response.status(200).send('light');
        }
      })
      .catch((ex) => {
        response.status(500).json(ex);
      });
  } catch (ex) {
    response.status(500).json(ex);
  }
}