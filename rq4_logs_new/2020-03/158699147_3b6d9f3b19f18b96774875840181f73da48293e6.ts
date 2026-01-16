type RecursivePartial<T> = {
    [P in keyof T]?: RecursivePartial<T[P]>;
};

export interface FailureConfig {
    meEndpoint: boolean;
    aktorIdEndpoint: boolean;
    websocketConnection: boolean;
    contextholder: {
        updateEnhet: boolean;
        updateBruker: boolean;
        deleteEnhet: boolean;
        deleteBruker: boolean;
        getEnhet: boolean;
        getBruker: boolean;
        get: boolean;
    };
}

const config: FailureConfig = {
    meEndpoint: true,
    aktorIdEndpoint: true,
    websocketConnection: true,
    contextholder: {
        updateEnhet: true,
        updateBruker: true,
        deleteEnhet: true,
        deleteBruker: true,
        getEnhet: true,
        getBruker: true,
        get: true
    }
};

export default config;