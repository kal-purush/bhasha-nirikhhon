function getEnv(key, _default, type = 's') {
    if (!!process.env[key] === false) {
        return _default;
    }
    const value = process.env[key];
    switch (type) {
        case 'b':
            return value.toLowerCase() === 'true';
        case 'n':
            return parseInt(value, 10);
        default:
            return value;
    }
}


export const DOMINIO = getEnv('IPS_DOMINIO', 'https://app.andes.gob.ar');
export const HOST = getEnv('IPS_HOST', 'https://testapp.hospitalitaliano.org.ar');
export const SECRET = getEnv('JWT_SECRET', '');
