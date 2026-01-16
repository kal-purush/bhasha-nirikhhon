import reducer from '../courses/reducer';
import { CourseType, CoursesActionTypes } from '../courses/types';

describe('Store', () => {
	it('reducer should return the initial state', () => {
		expect(
			reducer([] as CourseType[], {
				type: undefined,
				payload: [] as CourseType[],
			})
		).toEqual([]);
	});

	it('reducer should handle SAVE_COURSE and returns new state', () => {
		const previousState: CourseType[] = [];
		const coursesToSave: CourseType[] = [
			{
				id: '66cc289e-6de9-49b2-9ca7-8b4f409d6464',
				title: 'React JS',
				description: 'React JS Full Course.',
				duration: 10,
				creationDate: '22/10/2023',
				authors: [
					'9b87e8b8-6ba5-40fc-a439-c4e30a373d36',
					'9b87e8b8-6ba5-40fc-a439-c4e30a373d39',
				],
			},
		];

		expect(
			reducer(previousState, {
				type: CoursesActionTypes.SAVE_COURSES,
				payload: coursesToSave,
			})
		).toEqual(coursesToSave);
	});
});