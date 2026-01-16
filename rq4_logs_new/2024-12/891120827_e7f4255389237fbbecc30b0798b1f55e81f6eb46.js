import { describe, it, expect, beforeEach, vi } from "vitest";
import { migrateTasksToSupabase } from "../../lib/taskMigration";
import { STORAGE_KEY } from "../../utils/constants";

describe("taskMigration", () => {
  const mockUser = { id: "user123" };
  const mockTasks = [
    {
      id: 1,
      title: "Task 1",
      location: { type: "week", day: "Lundi", period: "morning", position: 0 },
    },
    {
      id: 2,
      title: "Task 2",
      location: { type: "parking", position: 0 },
    },
  ];

  const mockSupabase = {
    from: vi.fn(() => ({
      insert: vi.fn().mockResolvedValue({ data: mockTasks, error: null }),
    })),
  };

  beforeEach(() => {
    localStorage.clear();
    vi.clearAllMocks();
    localStorage.getItem.mockReturnValue(JSON.stringify(mockTasks));
  });

  it("should successfully migrate tasks from localStorage to Supabase", async () => {
    // Act
    const result = await migrateTasksToSupabase(mockUser, mockSupabase);

    // Assert
    expect(result.success).toBe(true);
    expect(result.data).toEqual(mockTasks);
    expect(localStorage.removeItem).toHaveBeenCalledWith(STORAGE_KEY);
    expect(mockSupabase.from).toHaveBeenCalledWith("tasks");
  });

  it("should handle migration errors gracefully", async () => {
    // Arrange
    const mockError = new Error("Migration failed");
    mockSupabase.from.mockImplementationOnce(() => ({
      insert: vi.fn().mockRejectedValue(mockError),
    }));

    // Act
    const result = await migrateTasksToSupabase(mockUser, mockSupabase);

    // Assert
    expect(result.success).toBe(false);
    expect(result.error).toBe(mockError.message);
    expect(localStorage.removeItem).not.toHaveBeenCalled();
  });

  it("should handle empty localStorage", async () => {
    // Arrange
    localStorage.getItem.mockReturnValue("[]");

    // Act
    const result = await migrateTasksToSupabase(mockUser, mockSupabase);

    // Assert
    expect(result.success).toBe(true);
    expect(mockSupabase.from).toHaveBeenCalledWith("tasks");
    expect(localStorage.removeItem).toHaveBeenCalledWith(STORAGE_KEY);
  });
});