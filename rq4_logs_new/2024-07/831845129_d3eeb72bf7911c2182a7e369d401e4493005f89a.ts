// utils/auth.ts
export const isAuthenticated = () => {
    if (typeof window !== 'undefined') {
        return sessionStorage.getItem('user') === 'true';
    }
    return false;
};