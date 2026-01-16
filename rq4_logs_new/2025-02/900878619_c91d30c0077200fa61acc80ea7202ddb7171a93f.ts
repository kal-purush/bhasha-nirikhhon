type TranslationObject = {
  enUS: string;
  ptBR: string;
};

type TranslationCollection = {
  [key: string]: TranslationObject;
};

const baseCollection = {
  cancelButton: {
    enUS: 'Cancel',
    ptBR: 'Cancelar',
  },
  submitButton: {
    enUS: 'Submit',
    ptBR: 'Enviar',
  },
} satisfies TranslationCollection;

export const makeCollection = <T>(collection: T): typeof baseCollection & T => {
  return { ...collection, ...baseCollection } satisfies TranslationCollection;
};

const localCollection = makeCollection({
  pageTitle: {
    enUS: 'Page Title',
    ptBR: 'Título da página',
  },
});

export function makeTranslator<TCollection extends TranslationCollection>(
  collection: TCollection
) {
  return function (
    key: keyof TCollection, // strictly infer keys from the provided collection
    language?: keyof TranslationObject
  ): string {
    return collection[key][language ?? 'enUS'];
  };
}

const t = makeTranslator(localCollection);