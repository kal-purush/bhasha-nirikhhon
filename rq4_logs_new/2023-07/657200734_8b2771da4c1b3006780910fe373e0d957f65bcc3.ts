/**
 * Controlla se un oggetto è valido rispetto ad uno schema, se è valido aggiunge il tipo all'oggetto
 * in modo che typescript lo riconosca come tale
 *
 * @param model modello da analizzare
 * @param schema schema da utilizzare per validare il modello
 * @returns true se il modello è valido, false altrimenti
 */
export default function validateZodModel<T>(
  model: any,
  schema: Zod.Schema
): model is T {
  const result = schema.safeParse(model);
  if (result.success) {
    return true;
  }
  console.log(`Model is not valid: ${result.error}`, model);
  return false;
}