import { db } from '@rocket-house-productions/integration';

interface GetLessonProps {
  userId: string;
  childId: string;
  courseSlug: string;
  moduleSlug: string;
  lessonSlug: string;
}

export const getLesson = async ({ userId, courseSlug, moduleSlug, lessonSlug, childId }: GetLessonProps) => {
  try {
    const account = await db.account.findUnique({
      where: {
        userId: userId,
      },
      include: {
        children: true,
      },
    });

    if (!account) {
      throw new Error('User not found');
    }

    const course = await db.course.findUnique({
      where: {
        slug: courseSlug,
      },
    });

    if (!course) {
      throw new Error('Course not found');
    }

    const module = await db.module.findUnique({
      where: {
        slug: moduleSlug,
        courseId: course.id,
      },
    });

    if (!module) {
      throw new Error('Module not found');
    }

    const purchase = await db.purchase.findUnique({
      where: {
        accountId_courseId_childId: {
          accountId: account.id,
          courseId: course.id,
          childId: childId,
        },
      },
    });

    if (!purchase) {
      throw new Error('Purchase not found');
    }

    const lesson = await db.lesson.findUnique({
      where: {
        slug: lessonSlug,
        moduleId: module.id,
        isPublished: true,
      },
    });

    if (!lesson) {
      throw new Error('Lesson not found');
    }

    // Check is free lesson

    // Get video
    // Get quiz
    // Get Awards

    // Get user progress
    const childProgress = await db.childProgress.findUnique({
      where: {
        childId_lessonId: {
          childId: childId,
          lessonId: lesson.id,
        },
      },
    });

    return {
      lesson,
      childProgress,
    };
  } catch (error) {
    console.error('Error getting lesson', error);
    return null;
  }
};

export default getLesson;