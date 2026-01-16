/**
 * @param {"http" | "https"} prefix URL prefix based on in-app SSL usage
 * @param {number} port Server port number
 * @returns {string[]} Array of pretty-printed network addresses
 */
export function getNetworkAddressesList(prefix: "http" | "https", port: number): string[];