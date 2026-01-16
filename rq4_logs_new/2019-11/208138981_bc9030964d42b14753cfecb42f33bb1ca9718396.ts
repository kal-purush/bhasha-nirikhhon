import { ClassroomModel, UserModel } from '../database';
import { classRoomDoc4, classRoomDoc5, user1 } from '../database/mockData';
import {
    authenticateToClassroom,
    createClassroom,
    getStudents,
} from '../service/classroomService';

const mockingoose = require('mockingoose').default;

test('Check if createClassroom creates classroom correctly', async () => {
    mockingoose(ClassroomModel).toReturn(classRoomDoc4, 'save');
    try {
        const classRoom = await createClassroom(classRoomDoc4);
        expect(JSON.parse(JSON.stringify(classRoom))).toMatchObject(
            classRoomDoc4,
        );
    } catch (e) {
        expect(e).toEqual(
            'A classroom already exists with this given name and passcode',
        );
    }
});

test('Check if authenticateToClassroom returns classroomId properly', async () => {
    mockingoose(ClassroomModel).toReturn(classRoomDoc4, 'findOne');
    try {
        const classRoomId = await authenticateToClassroom(classRoomDoc4);
        expect(classRoomId).toEqual('5dd0ddeaa1608611fdb1bb40');
    } catch (e) {
        expect(e).toEqual('Invalid classroom name or passcode');
    }
});

test('Check if authenticateToClassroom validates classroom properly - Should throw an error', async () => {
    mockingoose(ClassroomModel).toReturn(
        {
            name: 'Test105',
            numDailyProblems: 20,
            onlineResources: '',
            passcode: '2',
            problemsForToday: ['addition'],
        },
        'findOne',
    );
    try {
        await authenticateToClassroom(classRoomDoc5);
    } catch (e) {
        const err = JSON.parse(JSON.stringify(e));
        expect(err.output.payload.message).toEqual('WRONG_CLASS_INFO');
    }
});

test('Check if getStudents returns students correctly', async () => {
    mockingoose(UserModel).toReturn(user1, 'find');
    const students = await getStudents('113015909143620944320');
    expect(JSON.parse(JSON.stringify(students))).toMatchObject(user1);
});