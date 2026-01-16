import type { HttpContext } from '@adonisjs/core/http'
import User from '#models/user'
import { registerValidator, loginValidator } from '#validators/auth'
import type { AccessToken } from '@adonisjs/auth/access_tokens'

/**
 * Contrôleur d'authentification pour gérer les actions d'utilisateur.
 */
export default class AuthController {
  /**
   * Affiche la liste de tous les utilisateurs.
   * @param {HttpContext} context - Le contexte de la requête HTTP
   * @returns {Promise<void>} Répond avec la liste des utilisateurs en JSON
   */
  public async index({ response }: HttpContext): Promise<void> {
    const users: User[] = await User.all()
    return response.json(users)
  }
  /**
   * Gère la connexion de l'utilisateur.
   * @param {HttpContext} context - Le contexte de la requête HTTP
   * @returns {Promise<void>} Répond avec le token d'accès et les informations de l'utilisateur
   */
  public async login({ request, response }: HttpContext): Promise<void> {
    const { email, password } = await request.validateUsing(loginValidator)
    const user: User = await User.verifyCredentials(email, password)
    const token: AccessToken = await User.accessTokens.create(user)
    return response.ok({
      token: token,
      ...user.serialize(),
    })
  }
  /**
   * Gère l'enregistrement d'un nouvel utilisateur.
   * @param {HttpContext} context - Le contexte de la requête HTTP
   * @returns {Promise<void>} Répond avec les informations de l'utilisateur créé
   */
  public async register({ request, response }: HttpContext): Promise<void> {
    const payload: { fullName: string; email: string; password: string } =
      await request.validateUsing(registerValidator)
    const user: User = await User.create(payload)
    return response.created(user)
  }
  /**
   * Gère la déconnexion de l'utilisateur.
   * @param {HttpContext} context - Le contexte de la requête HTTP
   * @returns {Promise<void>} Répond avec un message de confirmation de déconnexion
   */
  public async logout({ auth, response }: HttpContext): Promise<void> {
    const user: User = auth.getUserOrFail()
    const token: AccessToken['identifier'] | undefined = auth.user?.currentAccessToken.identifier
    if (!token) {
      return response.badRequest({ message: 'Token not found' })
    }
    await User.accessTokens.delete(user, token)
    return response.ok({ message: 'Logged out' })
  }
}