import { vi } from "vitest";

const mockModules = vi.hoisted(() => [
  {
    id: "1",
    name: "Started course",
    enabled: true,
    type: "course",
  },
  {
    id: "2",
    name: "Finished course",
    enabled: true,
    type: "course",
  },
  {
    id: "3",
    name: "Never started enabled course",
    enabled: true,
    type: "course",
  },
  {
    id: "4",
    name: "Never started disabled course",
    enabled: false,
    type: "course",
  },
]);

const mockProgress = vi.hoisted(() => [
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

const mockRepetitions = vi.hoisted(() => [
  {
    moduleId: "101",
    userId: "101",
    startedAt: 1,
    finishedAt: null,
  },
  {
    moduleId: "102",
    userId: "101",
    startedAt: 1,
    finishedAt: null,
  },
]);

vi.mock("@/services/progress", () => ({
  find: vi
    .fn()
    .mockImplementation((_, moduleId) =>
      mockProgress.find((e) => e.moduleId === moduleId)
    ),
}));
vi.mock("@/services/repetition", () => ({
  agenda: vi.fn().mockResolvedValue(mockRepetitions),
}));
vi.mock("@/services/modules", () => ({
  fetchChildren: vi.fn().mockResolvedValue(mockModules),
}));

import subject from "@/pages/Home/currentProgress";

describe("pages/currentProgress", () => {
  it("throws when uid is empty", async () => {
    await expect(() => subject("")).rejects.toThrow();
  });
  it("not throws when uid is not empty", async () => {
    const result = await subject("123");
    expect(result).toBeDefined();
  });
  it('finds exact 2 words to repeat', async () => {
    const result = await subject("123");
    expect(result.wordsToRepeat).toBe(2)
  })
  it('finds exact 4 courses', async () => {
    const result = await subject("123");
    expect(result.courses).toHaveLength(3)
  })

  it('skips disabled courses', async () => {
    const result = await subject("123");
    expect(result.courses.find(e => e.name.includes('disabled'))).not.toBeDefined()
  })

  it('finds 1 not stared course', async () => {
    const result = await subject("123");
    expect(result.courses.filter(e => !e.progress)).toHaveLength(1)
  })

});