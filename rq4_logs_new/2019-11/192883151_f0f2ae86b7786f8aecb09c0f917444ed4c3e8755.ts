export const SessionStorageElement = {
    BRUKER_TYPE: 'brukertype',
    PRIVAT_BRUKER: 'privatbruker',
    MANUELL_BRUKER: 'manuellbruker',
    KRR_BRUKER: 'krrbruker',
    INGEN_OPPF_PERIODER: 'ingen_oppf_perioder'
};

export const BRUKER_TYPE = {
    INTERN: 'intern',
    EKSTERN: 'ekstern'
};

export const settSessionStorage = (key: string, value: any) => {
    window.localStorage.setItem(key, value);
};

export const hentFraSessionStorage = (key: string) => {
    return window.localStorage.getItem(key);
};

const erSatt = (sessionStorageElement: string) => {
    return hentFraSessionStorage(sessionStorageElement) === 'true';
};

export const erPrivatBruker = () => erSatt(SessionStorageElement.PRIVAT_BRUKER);

export const erManuellBruker = () => erSatt(SessionStorageElement.MANUELL_BRUKER);

export const erKRRBruker = () => erSatt(SessionStorageElement.KRR_BRUKER);

export const ingenOppfPerioder = () => erSatt(SessionStorageElement.INGEN_OPPF_PERIODER);

export const erEksternBruker = () => hentFraSessionStorage(SessionStorageElement.BRUKER_TYPE) === BRUKER_TYPE.EKSTERN;