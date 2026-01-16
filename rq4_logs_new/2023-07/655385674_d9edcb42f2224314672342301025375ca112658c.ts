import * as modules from "@/services/modules";
import * as progress from "@/services/progress";

export default async function deleteCourseProgress(uid: string, moduleId: string): Promise<void> {
  const lessons = await modules.fetchChildren(moduleId)
  const ids = [moduleId, ...lessons.map(e => e.id)]
  const promises = ids.map(async (moduleId): Promise<void> => {
    if (moduleId) {
      const p = await progress.find(uid, moduleId)
      if (p) {
        await progress.destroy(p)
      }
    }
  })
  await Promise.all(promises)
}