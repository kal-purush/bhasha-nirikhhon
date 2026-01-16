import MockAdapter from 'axios-mock-adapter';

import { baseApi } from '@client/store/sagas/api/url';

const NUMBER_OF_STUDENTS = 7;
export const CODE_OK = 200;

const appData = {
    classroom: {
        name: 'APSC100',
        passcode: '1234',
        students: new Array(NUMBER_OF_STUDENTS).fill(undefined).map((_, i) => ({
            email: 'test@test.com',
            id: String(i),
            name: `Test Student ${i}`,
            pushToken: `token${i}`,
            registered: true,
            userType: 'student',
            virtualClassroomUid: 'classroom',
        })),
    },
    user: [
        {
            email: 'test@test.com',
            id: '1',
            name: 'Test Student',
            pushToken: 'token1',
            registered: true,
            userType: 'student',
            virtualClassroomUid: 'classroom',
        },
        {
            email: 'test@test.com',
            id: '2',
            name: 'Test Teacher',
            pushToken: 'token2',
            registered: true,
            userType: 'teacher',
            virtualClassroomUid: 'classroom',
        },
    ],
};

export const apiMockSetup = () => {
    const mock = new MockAdapter(baseApi);

    mock.onGet('/classroom/students').reply(() => [
        ,
        appData.classroom.students,
    ]);

    mock.onGet('/math/nextProblem').reply(() => [
        CODE_OK,
        {
            incorrectSolutions: ['5', '7', '8'],
            operands: [1, 1],
            operators: ['+'],
            problem: '1 + 1 =',
            problemType: 'addition',
            solution: ['2'],
        },
    ]);

    return mock;
};

export const noCurrentUser = (mock: MockAdapter) => {
    mock.onGet('/user/current').reply(CODE_OK, undefined);

    mock.onPost('/auth').reply(CODE_OK, {
        accessToken: 'access_token',
        user: {
            id: '12345678910',
            name: 'Test User',
            registered: false,
        },
    });
};

export const registerStudent = (mock: MockAdapter) => {
    mock.onPost('/user/register').reply(() => {
        return [
            CODE_OK,
            {
                accessToken: 'access_token',
                user: {
                    id: '12345678910',
                    name: 'Test User',
                    registered: true,
                    userType: 'student',
                    virtualClassroomUid: 'classroom',
                },
            },
        ];
    });
};

export const registerTeacher = (mock: MockAdapter) => {
    mock.onPost('/user/register').reply(() => {
        return [
            CODE_OK,
            {
                accessToken: 'access_token',
                user: {
                    id: '12345678910',
                    name: 'Test User',
                    registered: true,
                    userType: 'teacher',
                    virtualClassroomUid: 'classroom',
                },
            },
        ];
    });
};

export const loginStudent = (mock: MockAdapter) => {
    mock.onGet('/user/current').reply(CODE_OK, undefined);

    mock.onPost('/auth').reply(CODE_OK, {
        accessToken: 'access_token',
        user: appData.user[0],
    });
};

export const loginTeacher = (mock: MockAdapter) => {
    mock.onGet('/user/current').reply(CODE_OK, undefined);

    mock.onPost('/auth').reply(CODE_OK, {
        accessToken: 'access_token',
        user: appData.user[1],
    });
};