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

const defaultConfig: FailureConfig = {
    meEndpoint: false,
    aktorIdEndpoint: false,
    websocketConnection: false,
    contextholder: {
        updateEnhet: false,
        updateBruker: false,
        deleteEnhet: false,
        deleteBruker: false,
        getEnhet: false,
        getBruker: false,
        get: false
    }
};

export function getFailureConfig(config: RecursivePartial<FailureConfig>): FailureConfig {
    return {
        ...defaultConfig,
        ...config,
        contextholder: {
            ...defaultConfig.contextholder,
            ...config.contextholder
        }
    };
}