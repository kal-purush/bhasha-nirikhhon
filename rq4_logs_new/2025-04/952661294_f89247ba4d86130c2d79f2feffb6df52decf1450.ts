import { create } from "zustand";

import { ProjectEntity, FolderEntity, RequestEntity } from "@/@entities";

function updateRequestInProject(
  project: ProjectEntity,
  updatedRequest: RequestEntity
): ProjectEntity {
  const requests = project.requests?.map((r) =>
    r.id === updatedRequest.id ? updatedRequest : r
  );

  const folders = project.folders?.map((folder) => ({
    ...folder,
    requests: folder.requests?.map((r) =>
      r.id === updatedRequest.id ? updatedRequest : r
    ),
  }));

  return {
    ...project,
    requests,
    folders,
    updatedAt: new Date().toISOString(),
  };
}

interface ProjectStoreState {
  project: ProjectEntity | null;
  selectedRequest: RequestEntity | null;
  selectedFolder: FolderEntity | null;

  setProject: (project: ProjectEntity) => void;
  updateRequest: (request: RequestEntity) => void;
  updateFolder: (folder: FolderEntity) => void;
  updateProjectMeta: (data: Partial<Pick<ProjectEntity, "title" | "icon" | "description">>) => void;
  mergeProject: (data: Partial<ProjectEntity>) => void;

  setSelectedRequest: (request: RequestEntity | null ) => void;
  setSelectedFolder: (folder: FolderEntity | null) => void;
  resetSelections: () => void;
  clearProject: () => void;
}

export const useProjectStore = create<ProjectStoreState>((set, get) => ({
  project: null,
  selectedRequest: null,
  selectedFolder: null,

  setProject: (project) => set({ project }),

  updateRequest: (updatedRequest) => {
    const state = get();
    if (!state.project) return;

    const updatedProject = updateRequestInProject(state.project, updatedRequest);
    set({ project: updatedProject });
  },

  updateFolder: (updatedFolder) => {
    const state = get();
    if (!state.project) return;

    const folders = state.project.folders?.map((folder) =>
      folder.id === updatedFolder.id ? updatedFolder : folder
    );

    set({
      project: {
        ...state.project,
        folders,
        updatedAt: new Date().toISOString(),
      },
    });
  },

  updateProjectMeta: (data) => {
    const state = get();
    if (!state.project) return;

    set({
      project: {
        ...state.project,
        ...data,
        updatedAt: new Date().toISOString(),
      },
    });
  },

  mergeProject: (data) => {
    const state = get();
    if (!state.project) return;

    set({
      project: {
        ...state.project,
        ...data,
        updatedAt: new Date().toISOString(),
      },
    });
  },

  setSelectedRequest: (request) => set({ selectedRequest: request }),
  setSelectedFolder: (folder) => set({ selectedFolder: folder }),
  resetSelections: () => set({ selectedRequest: null, selectedFolder: null }),
  clearProject: () =>
    set({ project: null, selectedRequest: null, selectedFolder: null }),
}));