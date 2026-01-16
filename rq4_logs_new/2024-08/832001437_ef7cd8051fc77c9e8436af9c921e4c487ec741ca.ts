import create, { createStore } from 'zustand';
import useModuleStore from './use-module-progress-store';

interface Course {
  progress: number;
  modules: string[]; // Array of module IDs
}

interface CourseState {
  courses: { [courseId: string]: Course };
  setCourseProgress: (courseId: string, progress: number) => void;
  calculateCourseProgress: (courseId: string) => void;
}

export type CourseProgress = CourseState;

export const useCourseStore = createStore<CourseState>((set, get) => ({
  courses: {},

  setCourseProgress: (courseId, progress) =>
    set(state => ({
      courses: {
        ...state.courses,
        [courseId]: {
          ...state.courses[courseId],
          progress,
        },
      },
    })),

  calculateCourseProgress: courseId => {
    const course = get().courses[courseId];
    const moduleStore = useModuleStore.getState();

    const modules = course?.modules || [];
    const totalModules = modules.length;
    const completedModules = modules.filter(moduleId => moduleStore.modules[moduleId]?.progress === 100).length;

    const progress = (completedModules / totalModules) * 100;

    set(state => ({
      courses: {
        ...state.courses,
        [courseId]: {
          ...state.courses[courseId],
          progress,
        },
      },
    }));
  },
}));