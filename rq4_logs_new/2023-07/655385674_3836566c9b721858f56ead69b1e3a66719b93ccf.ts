import { vi } from "vitest";

const mockModules = vi.hoisted(() => [
  {
    id: "1",
    parentId: '101',
    name: "Lesson 1",
    enabled: true,
    type: "lesson",
  },
  {
    id: "2",
    parentId: '101',
    name: "Lesson 2",
    enabled: true,
    type: "lesson",
  },
]);

const mockProgress = vi.hoisted(() => [
  {
    moduleId: "101",
    userId: "101",
    startedAt: 1,
    finishedAt: null,
  },
  {
    moduleId: "1",
    userId: "101",
    startedAt: 1,
    finishedAt: null,
  },
  {
    moduleId: "2",
    userId: "101",
    startedAt: 1,
    finishedAt: 1,
  },
]);
vi.mock("@/services/progress", () => ({
  find: vi
    .fn()
    .mockImplementation((_, moduleId) =>
      mockProgress.find((e) => e.moduleId === moduleId)
    ),
  destroy: vi
  .fn()
  .mockResolvedValueOnce(true)
}));
vi.mock("@/services/modules", () => ({
  fetchChildren: vi.fn().mockResolvedValue(mockModules),
}));


import subject from "@/pages/Students/deleteCourseProgress"
import * as modules from "@/services/modules";
import * as progress from "@/services/progress";

describe('/pages/deleteCourseProgress', () => {
  it('just works', async () => {
    await subject('101', '101')
    expect(modules.fetchChildren).toHaveBeenCalled()
    expect(progress.find).toHaveBeenCalledTimes(3)
    expect(progress.destroy).toHaveBeenCalledTimes(3)
  })
})