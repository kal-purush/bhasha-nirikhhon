import { NextResponse } from 'next/server';
import { ZodError } from 'zod';

export function errorHandler(err: unknown, statusCode: number = 500) {
  console.error('🔴 Erreur capturée :', err);

  // Si l'erreur est une instance de `Error`
  if (err instanceof Error) {
    return NextResponse.json({ error: err.message }, { status: statusCode });
  }

  // Si l'erreur est une `ZodError` (problème de validation)
  if (err instanceof ZodError) {
    return NextResponse.json(
      {
        error: 'Erreur de validation',
        details: err.errors,
      },
      { status: 400 },
    );
  }

  // Si l'erreur est un objet contenant un message d'erreur
  if (typeof err === 'object' && err !== null && 'message' in err) {
    return NextResponse.json({ error: String((err as any).message) }, { status: statusCode });
  }

  // Si l'erreur est une chaîne (ex: `throw "Problème détecté"`)
  if (typeof err === 'string') {
    return NextResponse.json({ error: err }, { status: statusCode });
  }

  // Cas par défaut (erreur inconnue)
  return NextResponse.json({ error: 'Une erreur inconnue est survenue' }, { status: statusCode });
}