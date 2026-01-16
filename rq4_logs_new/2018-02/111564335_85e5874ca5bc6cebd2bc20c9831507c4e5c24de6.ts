import { SoknadActionKeys, SoknadActionTypes } from './soknadActionDefinitions';
import { ArbeidSiste12 } from '../../../types/Medlemsskap';

export function setAntallBarn(antallBarn: number): SoknadActionTypes {
    return {
        type: SoknadActionKeys.SET_ANTALL_BARN,
        antallBarn
    }
}

export function setFodselsdato(fodselsdato: string): SoknadActionTypes {
    return {
        type: SoknadActionKeys.SET_FODSELSDATO,
        fodselsdato
    }
}

export function setTerminDato(terminDato: string): SoknadActionTypes {
    return {
        type: SoknadActionKeys.SET_TERMIN_DATO,
        terminDato
    }
}

export function setUtstedtDato(utstedtDato: string): SoknadActionTypes {
    return {
        type: SoknadActionKeys.SET_UTSTEDT_DATO,
        utstedtDato
    }
}

export function setArbeidSiste12(arbeidSiste12: ArbeidSiste12): SoknadActionTypes {
    return {
        type: SoknadActionKeys.SET_ARBEID_SISTE_12,
        arbeidSiste12
    }
}

export function setFodselINorge(fodselINorge: boolean): SoknadActionTypes {
    return {
        type: SoknadActionKeys.SET_FODSEL_I_NORGE,
        fodselINorge
    }
}

export function setINorgeSiste12(iNorgeSiste12: boolean): SoknadActionTypes {
    return {
        type: SoknadActionKeys.SET_I_NORGE_SISTE_12,
        iNorgeSiste12
    }
}

export function setINorgeNeste12(iNorgeNeste12: boolean): SoknadActionTypes {
    return {
        type: SoknadActionKeys.SET_I_NORGE_NESTE_12,
        iNorgeNeste12
    }
}