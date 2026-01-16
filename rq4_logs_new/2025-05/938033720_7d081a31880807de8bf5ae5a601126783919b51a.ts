import { z } from "zod";

const requireFieldErrorMessage = "Feltet er påkrevd";

export const OppfolgingsplanFormSchema = z.object({
  typiskArbeidshverdag: z.string().nonempty(requireFieldErrorMessage),
  arbeidsoppgaverSomKanUtfores: z.string().nonempty(requireFieldErrorMessage),
  arbeidsoppgaverSomIkkeKanUtfores: z
    .string()
    .nonempty(requireFieldErrorMessage),
  tidligereTilretteleggingBeskrivelse: z
    .string()
    .nonempty(requireFieldErrorMessage),
  tilretteleggingIDennePerioden: z.string().nonempty(requireFieldErrorMessage),
  sykmeldtesVurdering: z.string().nonempty(requireFieldErrorMessage),
  oppfolging: z.string().nonempty(requireFieldErrorMessage),
  evalueringsdato: z
    .date()
    .nullable()
    .refine((val) => val instanceof Date && !isNaN(val.getTime()), {
      message: requireFieldErrorMessage,
    }),
});

export type OppfolgingsplanFormFields = z.infer<
  typeof OppfolgingsplanFormSchema
>;