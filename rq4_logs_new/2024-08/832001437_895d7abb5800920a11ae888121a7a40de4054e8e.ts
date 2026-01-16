import { createStore, StoreApi } from 'zustand';
import { LessonProgressStore } from './use-lesson-progress-store';

type Module = {
  progress: number;
  lessons: string[]; // Array of lesson IDs
};

type ModuleState = {
  modules: { [moduleId: string]: Module };
};

type ModuleAction = {
  setModuleProgress: (moduleId: string, progress: number) => void;
  calculateModuleProgress: (moduleId: string) => void;
  getModuleProgress: (moduleId: string) => number;
};

export type ModuleProgressStore = ModuleState & ModuleAction;

export const defaultInitState: ModuleState = {
  modules: {},
};

export const createModuleStore = (
  lessonState: StoreApi<LessonProgressStore>,
  initState: ModuleState = defaultInitState,
) =>
  createStore<ModuleProgressStore>((set, get) => ({
    ...initState,

    setModuleProgress: (moduleId, progress) =>
      set(state => ({
        modules: {
          ...state.modules,
          [moduleId]: {
            ...state.modules[moduleId],
            progress,
          },
        },
      })),

    calculateModuleProgress: moduleId => {
      const module = get().modules[moduleId];
      const lessonStore = lessonState.getState();

      console.log('MODULE STORE: ', lessonStore);

      const lessons = module?.lessons || [];
      const totalLessons = lessons.length;
      const completedLessons = lessons.filter(lessonId => lessonStore.lessons[lessonId]?.progress === 100).length;

      const progress = (completedLessons / totalLessons) * 100;

      set(state => ({
        modules: {
          ...state.modules,
          [moduleId]: {
            ...state.modules[moduleId],
            progress,
          },
        },
      }));
    },
    getModuleProgress: moduleId => get().modules[moduleId]?.progress || 0,
  }));