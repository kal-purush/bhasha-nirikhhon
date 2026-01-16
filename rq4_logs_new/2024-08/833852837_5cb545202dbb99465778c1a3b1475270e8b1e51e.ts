import { getState, patchState, signalStore, type, withComputed, withHooks, withMethods, withState } from "@ngrx/signals";
import { removeAllEntities, setAllEntities, withEntities } from '@ngrx/signals/entities';
import { Course, Lesson } from "../common/interfaces";
import { effect } from "@angular/core";

export type FriendlyChatState = {
    selectedCourseId: string,
    selectedLessonId: string,
}

const initialState: FriendlyChatState = {
    selectedCourseId: '',
    selectedLessonId: '',
}


export const FriendlyChatStore = signalStore(
    {providedIn: 'root'},
    withState(initialState),
    withEntities({ entity: type<Course>(), collection: 'course' }),
    withEntities({ entity: type<Lesson>(), collection: 'lesson' }),
    withComputed(() => ({})),
    withMethods((store) => ({
        
        setAllCourses(courses: Course[]) {
            patchState(store, setAllEntities([...courses], {collection: 'course', idKey: 'id'}));
        },

        setSelectedCourseId(selectedCourseId: string) {
            patchState(store, {selectedCourseId});
        },

        setAllLessons(lessons: Lesson[]) {
            patchState(store, setAllEntities([...lessons], {collection: 'lesson', idKey: 'id'}));
        },

        setSelectedLessonId(selectedLessonId: string) {
            patchState(store, {selectedLessonId});
        },

    })),

    withHooks({
        // onInit(store) {
        //     effect(() => {
        //         const state = getState(store);
        //         console.log('fCSto oI friendly chat state eff: ', state);
        //     });
        // }
    }),

);