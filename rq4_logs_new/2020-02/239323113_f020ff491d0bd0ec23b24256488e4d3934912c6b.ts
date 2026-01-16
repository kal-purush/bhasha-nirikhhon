import * as request from "request-promise-native";
import { Request} from "express";


export const getCurrentUser = async (req: Request, path: String): Promise<String> => {
    return request.get(`${path}/api/v1/users/me`, {
        headers: {
          Authorization: `${req.cookies["Authorization"]}`,
          Referer: req.originalUrl
        }
      });
}

export const getAllUsers = async (req: Request, path: String): Promise<any> => {
    request.get(`${path}/api/v1/users`, {
        headers: {
          Authorization: `${req.cookies["Authorization"]}`
        }
      });
}

export const putCurrentUser = async (req: Request, path: String, name: String): Promise<void> => {
    request.put(`${path}/api/v1/users/me`, {
        headers: {
            Authorization: `${req.cookies["Authorization"]}`
        },
        body: {
            name: `${name}`
        }
    });
}

export const getTeams = async (req: Request, path: String, ): Promise<String> => {
    return request.get(`${path}/api/v1/teams/`, {
        headers: {
          Authorization: `${req.cookies["Authorization"]}`
        }
    });
}

export const getTeam = async (req: Request, path: String, teamCode: String ): Promise<String> => {
  return request.get(`${path}/api/v1/teams/${teamCode}`, {
      headers: {
        Authorization: `${req.cookies["Authorization"]}`
      }
  });
}

export const getTeamMembers = async (req: Request, path: String, teamCode: String): Promise<String> => {
    return request.get(`${path}/api/v1/teams/${teamCode}/members`, {
        headers: {
          Authorization: `${req.cookies["Authorization"]}`
        }
    });
}