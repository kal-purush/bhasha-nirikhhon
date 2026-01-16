import { db } from '@rocket-house-productions/integration';
import { notFound, redirect } from 'next/navigation';

export const getChild = async (slug: string) => {
  const purchase = await db.purchase.findFirst({
    where: {
      course: {
        slug: slug,
      },
    },
  });

  if (!purchase) {
    return notFound();
  }

  if (!purchase.childId) {
    return redirect(`/courses/error?status=error&message=No%20child%20found`);
  }

  const child = await db.child.findFirst({
    where: {
      id: purchase?.childId,
    },
  });

  if (!child) {
    return redirect(`/courses/error?status=error&message=No%20child%20found`);
  }

  return child;
};