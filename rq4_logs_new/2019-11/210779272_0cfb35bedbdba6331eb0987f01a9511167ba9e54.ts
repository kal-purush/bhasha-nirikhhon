/** @property {string} regex pattern to identify students */
const regex = /^[a-z][0-9][a-z]{1,6}/;

/**
 * @summary Method used to check if given casUser match student pattern
 * @param {string} casUser cas username
 * @returns {boolean} check result
 */
export function isStudent(casUser: string): boolean {
    if (regex.test(casUser)) {
        return true;
    }
    return false;
}

/**
 * @summary Method used to get email of given cas user
 * @param {string} casUser cas username
 * @returns {string} email
 */
export function getEmail(casUser: string): string {
    return `${casUser}@enib.fr`;
}