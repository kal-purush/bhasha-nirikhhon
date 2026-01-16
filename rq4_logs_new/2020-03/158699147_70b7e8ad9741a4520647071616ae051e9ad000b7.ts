import { DataAction } from '../utils';

export enum FeilmeldingKode {
    VALIDERING_FNR = 'A001',
    UKJENT_VALIDERING_FNR = 'A002',
    INGEN_GYLDIG_ENHET = 'A100',
    HENT_SAKSBEHANDLER_DATA_FEILET = 'A200',
    HENT_BRUKER_CONTEXT = 'A300',
    HENT_AKTORID_FEILET = 'A400',
    WS_FEILET = 'A500'
}

export interface Feilmelding {
    kode: FeilmeldingKode;
    melding: string;
}

export enum FeilmeldingActionTypes {
    LEGG_TIL = 'REDUX/FEILMELDING/LEGG_TIL',
    FJERN = 'REDUX/FEILMELDING/FJERN'
}

export type LeggTilFeilmelding = DataAction<FeilmeldingActionTypes.LEGG_TIL, Feilmelding>;
export type FjernFeilmelding = DataAction<FeilmeldingActionTypes.FJERN, FeilmeldingKode>;

export type FeilmeldingerActions = LeggTilFeilmelding | FjernFeilmelding;