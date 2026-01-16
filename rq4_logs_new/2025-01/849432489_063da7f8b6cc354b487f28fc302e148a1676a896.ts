import jwtDecode from 'jwt-decode';

/**
 * Decodifica o token JWT e retorna os dados decodificados.
 * @param token O token JWT.
 * @returns Os dados decodificados ou null se o token for inválido.
 */
export function decodeToken(token: string) {
    try {
        return jwtDecode<{ isGestao: boolean }>(token); // Decodifica o token e retorna o payload
    } catch (error) {
        return null; // Retorna null se o token não puder ser decodificado
    }
}