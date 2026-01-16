export enum SoknadActionKeys {
    // RelasjonTilBarn
    'SET_ANTALL_BARN' = 'setAntallBarn',
    'SET_FODSELSDATO' = 'setFodselsdato',
    'SET_TERMIN_DATO' = 'setTerminDato',
    'SET_UTSTEDT_DATO' = 'setUtstedtDato',

    // Medlemsskap
    'SET_ARBEID_SISTE_12' = 'setArbeidSiste12',
    'SET_I_NORGE_SISTE_12' = 'setINorgeSiste12',
    'SET_I_NORGE_NESTE_12' = 'setINorgeNeste12',
    'SET_FODSEL_I_NORGE' = 'setFodselINorge'
}

// RelasjonTilBarn
export interface SetAntallBarn {
    type: SoknadActionKeys.SET_ANTALL_BARN;
    antallBarn: number;
}

export interface SetFodselsdato {
    type: SoknadActionKeys.SET_FODSELSDATO;
    fodselsdato: string;
}

export interface SetTerminDato {
    type: SoknadActionKeys.SET_TERMIN_DATO;
    terminDato: string;
}

export interface SetUtstedtDato {
    type: SoknadActionKeys.SET_UTSTEDT_DATO;
    utstedtDato: string;
}

// Medlemsskap
export interface SetArbeidSiste12 {
    type: SoknadActionKeys.SET_ARBEID_SISTE_12;
    antallBarn: number;
}

export interface SetINorgeSiste12 {
    type: SoknadActionKeys.SET_I_NORGE_SISTE_12;
    fodselsdato: string;
}

export interface SetINorgeNeste12 {
    type: SoknadActionKeys.SET_I_NORGE_NESTE_12;
    terminDato: string;
}

export interface SetFodselINorge {
    type: SoknadActionKeys.SET_FODSEL_I_NORGE;
    utstedtDato: string;
}