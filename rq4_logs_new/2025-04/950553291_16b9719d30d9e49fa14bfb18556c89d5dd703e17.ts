// tests/issuesSlice.test.ts
import { describe, test, expect } from "vitest";
import reducer, {
  setActiveId,
  dragOver,
  dragEnd,
} from "../app/store/slices/issuesSlice";
import { IssuesState, Container, Issue } from "../app/types/types";
import  {createMockIssue}  from "./test-utils";

const mockState: IssuesState = {
  repo: null,
  containers: [
    {
      id: "todo",
      title: "To Do",
      items: [createMockIssue(1, "user1")],
    },
    {
      id: "done",
      title: "Done",
      items: [],
    },
  ],
  loading: true,
  error: null,
  activeId: null,
};

describe("issuesSlice", () => {
  test("should set activeId", () => {
    const state = reducer(mockState, setActiveId(123));
    expect(state.activeId).toBe(123);
  });

  test("should handle dragOver between containers", () => {
    const state = reducer(mockState, dragOver({ activeId: 1, overId: "done" }));
    expect(state.containers.find((c) => c.id === "todo")!.items.length).toBe(0);
    expect(state.containers.find((c) => c.id === "done")!.items.length).toBe(1);
  });

  test("should handle dragEnd in same container with reorder", () => {
    const modifiedState = {
      ...mockState,
      containers: [
        {
          id: "todo",
          title: "To Do",
          items: [createMockIssue(1, "user1"), createMockIssue(2, "user2")           
          ],
        },
        { id: "done", title: "Done", items: [] },
      ],
    };

    const state = reducer(modifiedState, dragEnd({ activeId: 1, overId: 2 }));

    const ids = state.containers
      .find((c) => c.id === "todo")!
      .items.map((i) => i.id);
    expect(ids).toEqual([2, 1]);
  });
});