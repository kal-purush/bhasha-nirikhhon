import * as modules from "@/services/modules";
import * as progress from "@/services/progress";
import * as repetition from "@/services/repetition";

import type { Module, ProgressRecord } from "@/types";

export type CourseProgress = Module & { progress?: ProgressRecord };

type ProgressResponse = {
  courses: CourseProgress[];
  wordsToRepeat: number;
};

export default async function currentProgress(
  uid: string
): Promise<ProgressResponse> {
  const loadCoursesPromise = modules.fetchChildren("");
  const loadAgendaPromise = repetition.agenda(uid);
  const [courses, agenda] = await Promise.all([
    loadCoursesPromise,
    loadAgendaPromise,
  ]);

  const wordsToRepeat = agenda.length;

  const promises = courses
    .map((e) => e.id && progress.find(uid, e.id))
    .filter(Boolean);
  const records = (await Promise.all(promises)).filter(
    Boolean
  ) as ProgressRecord[];

  return {
    courses: courses
      .filter((e) => e.enabled)
      .map((e) => {
        const progress = records.find((r) => r.moduleId === e.id);
        return { ...e, progress };
      }),
    wordsToRepeat,
  };
}