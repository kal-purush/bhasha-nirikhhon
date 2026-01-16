import { z } from 'zod';

const HarArbeidsgiver = z.enum(['EN_ARBEIDSGIVER', 'FLERE_ARBEIDSGIVERE', 'INGEN_ARBEIDSGIVER']);
type HarArbeidsgiver = z.infer<typeof HarArbeidsgiver>;

export const HarArbeidsgiverValues: Record<HarArbeidsgiver, string> = {
  EN_ARBEIDSGIVER: 'Én arbeidsgiver',
  FLERE_ARBEIDSGIVERE: 'Flere arbeidsgivere',
  INGEN_ARBEIDSGIVER: 'Ingen arbeidsgiver',
};

export const Arbeidsgiver = z.object({
  harArbeidsgiver: HarArbeidsgiver,
  navn: z.string().nullable(),
  yrkesbetegnelse: z.string().nullable(),
  stillingsprosent: z.number().nullable(),
});
export type Arbeidsgiver = z.infer<typeof Arbeidsgiver>;

// --------

export const Diagnose = z.object({
  system: z.string(),
  kode: z.string(),
  tekst: z.string(),
});
export type Diagnose = z.infer<typeof Diagnose>;

const AnnenFraverGrunn = z.enum([
  'GODKJENT_HELSEINSTITUSJON',
  'BEHANDLING_FORHINDRER_ARBEID',
  'ARBEIDSRETTET_TILTAK',
  'MOTTAR_TILSKUDD_GRUNNET_HELSETILSTAND',
  'NODVENDIG_KONTROLLUNDENRSOKELSE',
  'SMITTEFARE',
  'ABORT',
  'UFOR_GRUNNET_BARNLOSHET',
  'DONOR',
  'BEHANDLING_STERILISERING',
]);
type AnnenFraverGrunn = z.infer<typeof AnnenFraverGrunn>;

export const AnnenFraverGrunnValues: Record<AnnenFraverGrunn, string> = {
  GODKJENT_HELSEINSTITUSJON: 'Når vedkommende er innlagt i en godkjent helseinstitusjon',
  BEHANDLING_FORHINDRER_ARBEID:
    'Når vedkommende er under behandling og legen erklærer at behandlingen gjør det nødvendig at vedkommende ikke arbeider',
  ARBEIDSRETTET_TILTAK: 'Når vedkommende deltar på et arbeidsrettet tiltak',
  MOTTAR_TILSKUDD_GRUNNET_HELSETILSTAND:
    'Når vedkommende på grunn av sykdom, skade eller lyte får tilskott når vedkommende på grunn av sykdom, skade eller lyte får tilskott',
  NODVENDIG_KONTROLLUNDENRSOKELSE:
    'Når vedkommende er til nødvendig kontrollundersøkelse som krever minst 24 timers fravær, reisetid medregnet',
  SMITTEFARE: 'Når vedkommende myndighet har nedlagt forbud mot at han eller hun arbeider på grunn av smittefare',
  ABORT: 'Når vedkommende er arbeidsufør som følge av svangerskapsavbrudd',
  UFOR_GRUNNET_BARNLOSHET: 'Når vedkommende er arbeidsufør som følge av behandling for barnløshet',
  DONOR: 'Når vedkommende er donor eller er under vurdering som donor',
  BEHANDLING_STERILISERING: 'Når vedkommende er arbeidsufør som følge av behandling i forbindelse med sterilisering',
};

const AnnenFraversArsak = z.object({
  beskrivelse: z.string().nullable(),
  grunn: z.array(AnnenFraverGrunn),
});

export const MedisinskVurdering = z.object({
  hovedDiagnose: Diagnose.nullable(),
  biDiagnoser: z.array(Diagnose),
  svangerskap: z.boolean(),
  yrkesskade: z.boolean(),
  yrkesskadeDato: z
    .string()
    .transform((arg) => new Date(arg))
    .nullable(),
  annenFraversArsak: AnnenFraversArsak.nullable(),
});
export type MedisinskVurdering = z.infer<typeof MedisinskVurdering>;

// --------

const MedisinskArsakType = z.enum([
  'TILSTAND_HINDRER_AKTIVITET',
  'AKTIVITET_FORVERRER_TILSTAND',
  'AKTIVITET_FORHINDRER_BEDRING',
  'ANNET',
]);
type MedisinskArsakType = z.infer<typeof MedisinskArsakType>;

export const MedisinskArsakTypeValues: Record<MedisinskArsakType, string> = {
  TILSTAND_HINDRER_AKTIVITET: 'Helsetilstanden hindrer pasienten i å være i aktivitet',
  AKTIVITET_FORVERRER_TILSTAND: 'Aktivitet vil forverre helsetilstanden',
  AKTIVITET_FORHINDRER_BEDRING: 'Aktivitet vil hindre/forsinke bedring av helsetilstanden',
  ANNET: 'Annet',
};

const MedisinskArsak = z.object({
  beskrivelse: z.string().nullable(),
  arsak: z.array(MedisinskArsakType),
});

const ArbeidsrelatertArsakType = z.enum(['MANGLENDE_TILRETTELEGGING', 'ANNET']);
type ArbeidsrelatertArsakType = z.infer<typeof ArbeidsrelatertArsakType>;

export const ArbeidsrelatertArsakTypeValues: Record<ArbeidsrelatertArsakType, string> = {
  MANGLENDE_TILRETTELEGGING: 'Manglende tilrettelegging på arbeidsplassen',
  ANNET: 'Annet',
};

const ArbeidsrelatertArsak = z.object({
  beskrivelse: z.string().nullable(),
  arsak: z.array(ArbeidsrelatertArsakType),
});

const AktivitetIkkeMulig = z.object({
  medisinskArsak: MedisinskArsak.nullable(),
  arbeidsrelatertArsak: ArbeidsrelatertArsak.nullable(),
});

const Gradert = z.object({
  reisetilskudd: z.boolean(),
  grad: z.number().nullable(),
});

export const Periode = z.object({
  fom: z.string().transform((arg) => new Date(arg)),
  tom: z.string().transform((arg) => new Date(arg)),
  aktivitetIkkeMulig: AktivitetIkkeMulig.nullable(),
  avventendeInnspillTilArbeidsgiver: z.string().nullable(),
  behandlingsdager: z.number().nullable(),
  gradert: Gradert.nullable(),
  reisetilskudd: z.boolean(),
});
export type Periode = z.infer<typeof Periode>;

// --------

export const Prognose = z.object({
  arbeidsforEtterPeriode: z.boolean(),
  hensynArbeidsplassen: z.string().nullable(),
  erIArbeid: z
    .object({
      egetArbeidPaSikt: z.boolean(),
      annetArbeidPaSikt: z.boolean(),
      arbeidFOM: z
        .string()
        .transform((arg) => new Date(arg))
        .nullable(),
      vurderingsdato: z
        .string()
        .transform((arg) => new Date(arg))
        .nullable(),
    })
    .nullable(),
  erIkkeIArbeid: z
    .object({
      arbeidsforPaSikt: z.boolean(),
      arbeidsforFOM: z
        .string()
        .transform((arg) => new Date(arg))
        .nullable(),
      vurderingsdato: z
        .string()
        .transform((arg) => new Date(arg))
        .nullable(),
    })
    .nullable(),
});
export type Prognose = z.infer<typeof Prognose>;

// --------

export const MeldingTilNAV = z.object({
  bistandUmiddelbart: z.boolean(),
  beskrivBistand: z.string().nullable(),
});
export type MeldingTilNAV = z.infer<typeof MeldingTilNAV>;

// --------

export const KontaktMedPasient = z.object({
  kontaktDato: z
    .string()
    .transform((arg) => new Date(arg))
    .nullable(),
  begrunnelseIkkeKontakt: z.string().nullable(),
});
export type KontaktMedPasient = z.infer<typeof KontaktMedPasient>;

// --------

const Adresse = z.object({
  gate: z.string().nullable(),
  postnummer: z.number().nullable(),
  kommune: z.string().nullable(),
  postboks: z.string().nullable(),
  land: z.string().nullable(),
});

export const Behandler = z.object({
  fornavn: z.string(),
  mellomnavn: z.string().nullable(),
  etternavn: z.string(),
  aktoerId: z.string(),
  fnr: z.string(),
  hpr: z.string().nullable(),
  her: z.string().nullable(),
  adresse: Adresse,
  tlf: z.string().nullable(),
});
export type Behandler = z.infer<typeof Behandler>;

// --------

const SvarRestriksjon = z.enum(['SKJERMET_FOR_ARBEIDSGIVER', 'SKJERMET_FOR_PASIENT', 'SKJERMET_FOR_NAV']);
type SvarRestriksjon = z.infer<typeof SvarRestriksjon>;

export const SvarRestriksjonValues: Record<SvarRestriksjon, string> = {
  SKJERMET_FOR_ARBEIDSGIVER: 'Informasjonen skal ikke vises arbeidsgiver',
  SKJERMET_FOR_PASIENT: 'Informasjonen skal ikke vises pasient',
  SKJERMET_FOR_NAV: 'Informasjonen skal ikke vises NAV',
};

export const SporsmalSvar = z.object({
  sporsmal: z.string(),
  svar: z.string(),
  restriksjoner: z.array(SvarRestriksjon),
});
export type SporsmalSvar = z.infer<typeof SporsmalSvar>;

export const UtdypendeOpplysninger = z.record(z.record(SporsmalSvar));
export type UtdypendeOpplysninger = z.infer<typeof UtdypendeOpplysninger>;

// --------

const AvsenderSystem = z.object({
  navn: z.string(),
  versjon: z.string(),
});

// --------

export const Sykmelding = z.object({
  id: z.string(),
  msgId: z.string(),
  pasientAktoerId: z.string(),
  medisinskVurdering: MedisinskVurdering,
  skjermesForPasient: z.boolean(),
  arbeidsgiver: Arbeidsgiver,
  perioder: z.array(Periode),
  prognose: Prognose.nullable(),
  utdypendeOpplysninger: UtdypendeOpplysninger,
  tiltakArbeidsplassen: z.string().nullable(),
  tiltakNAV: z.string().nullable(),
  andreTiltak: z.string().nullable(),
  meldingTilNAV: MeldingTilNAV.nullable(),
  meldingTilArbeidsgiver: z.string().nullable(),
  kontaktMedPasient: KontaktMedPasient,
  behandletTidspunkt: z.string().transform((arg) => new Date(arg)),
  behandler: Behandler,
  avsenderSystem: AvsenderSystem,
  syketilfelleStartDato: z
    .string()
    .transform((arg) => new Date(arg))
    .nullable(),
  signaturDato: z.string().transform((arg) => new Date(arg)),
  navnFastlege: z.string(),
});
export type Sykmelding = z.infer<typeof Sykmelding>;