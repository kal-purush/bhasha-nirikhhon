'use server';
import { auth } from '@clerk/nextjs/server';
import { db } from '@rocket-house-productions/integration';
import { redirect } from 'next/navigation';

interface GetCourseProps {
  courseSlug: string;
}

export const getCourse = async ({ courseSlug }: GetCourseProps) => {
  const { userId } = auth();

  if (!userId) {
    return redirect('/');
  }

  const course = await db.course.findFirst({
    where: {
      slug: courseSlug,
      isPublished: true,
    },
    include: {
      attachments: {
        include: {
          attachmentType: {
            select: {
              name: true,
            },
          },
        },
        orderBy: {
          createdAt: 'desc',
        },
      },
      modules: {
        where: {
          isPublished: true,
        },
        orderBy: {
          position: 'asc',
        },
        include: {
          attachments: {
            include: {
              attachmentType: {
                select: {
                  name: true,
                },
              },
            },
            orderBy: {
              createdAt: 'desc',
            },
          },
          lessons: {
            where: {
              isPublished: true,
            },
            include: {
              category: {
                select: {
                  name: true,
                },
              },
              questionaries: true,
            },
            orderBy: {
              position: 'asc',
            },
          },
        },
      },
    },
  });

  if (!course) {
    return redirect('/');
  }

  return course;
};