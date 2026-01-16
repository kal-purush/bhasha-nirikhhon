import * as restify from 'restify'
import { User, IUserDocument, IUserModel, UserRole } from '../../app/models/user.model'

export default function adminRequired (target: any, propertyKey: string, descriptor: PropertyDescriptor) {
    const originalMethod = descriptor.value
    descriptor.value = (req: restify.Request, res: restify.Response, next: restify.Next): void => {
        if (req.username && req.authorization && req.authorization.credentials) {
        return User.findByLogin(req.username)
            .then((user: IUserDocument) => {
                req.params.user = user
                if (user.role === UserRole.admin) {
                    return originalMethod(req, res, next)
                }
                return next(new restify.ForbiddenError('Permission denied'))
            })
            .catch(() => next(new restify.ForbiddenError('Permission denied')))
        }
        return next(new restify.UnauthorizedError('Unauthorized'))
    }
}