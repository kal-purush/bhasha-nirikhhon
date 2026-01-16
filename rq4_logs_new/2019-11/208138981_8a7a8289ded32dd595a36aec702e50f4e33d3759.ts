import Boom from 'boom';

import { ClassroomModel, MasteryModel } from '@server/database';
import { IUser } from '@shared/index';
import { convertStringToProblemType } from '@shared/models/problem';

/*
 * Learning Algorithm method
 * @params: UserPayload: IUser
 * @returns: An object with the next problem type and difficulty
 */
export const nextProblemTypeAndDifficulty = async (userPayload: IUser) => {
    if (!userPayload) {
        throw Boom.badRequest('Parameter userPayload to the method nextProblemTypeAndDifficulty can not be empty');
    }

    if(userPayload.userType !== 'student') {
        throw Boom.badRequest('UserType must be student');
    }

    if(!userPayload.virtualClassroomUid) {
        throw Boom.badData('virtualClassroomUid can not be null');
    }

    if(!userPayload.id) {
        throw Boom.badData('user id can not be null');
    }

    const nextProblemTypes = await findPossibleNextProblemTypes(userPayload.id);

    // last element of nextProblemTypes is the difficulty
    const difficulty = nextProblemTypes.pop();

    const problemsForToday = await getProblemsForClass(userPayload.virtualClassroomUid);

    // send the first matching between problemsForToday and nextProblemTypes
    if(problemsForToday.length !== 0){
        for(const item of nextProblemTypes){
            if(problemsForToday.indexOf(item) !== -1) {
                return {difficulty, problemType: item};
            }
        }
    }

    // if there's no match || problemsForToday is empty, send the first type of nextProblemTypes
    return {difficulty, problemType: nextProblemTypes[0]};
};


const findPossibleNextProblemTypes = async (studentId: string) => {
    const mastery = await MasteryModel.findById(studentId);
    if (!mastery) {
        throw Boom.notFound('could not find mastery associated with the student id');
    }

    const progress = mastery.progress;

    if(!progress) {
        throw Boom.badData('progress should not be empty');
    }

    let minDifficulty = 'g11h';

    const map = ['g1e','g1m','g1h','g2e','g2m','g2h','g3e','g3m','g3h',
                'g4e','g4m','g4h','g5e','g5m','g5h','g6e','g6m','g6h',
                'g7e','g7m','g7h','g8e','g8m','g8h','g9e','g9m','g9h',
                'g10e','g10m','g10h','g11e','g11m','g11h'];

    const problemTypes = Object.keys(progress);

    // find minimum difficulty
    for (const item of problemTypes){
        const problemType = convertStringToProblemType(item);
        const progressForProblemType = progress.get(problemType);

        if(typeof progressForProblemType === 'undefined') {
            throw Boom.badData('progress can not be undefined');
        }

        const difficulty = progressForProblemType.difficulty.toLowerCase();

        if(map.indexOf(difficulty) < map.indexOf(minDifficulty)) {
            minDifficulty = difficulty;
        }
    }

    const nextProblemTypes = [];

    // find all the problemTypes with minimum difficulty
    for (const item of problemTypes){
        const problemType = convertStringToProblemType(item);
        const progressForProblemType = progress.get(problemType);

        if(typeof progressForProblemType === 'undefined') {
            throw Boom.badData('progress can not be undefined');
        }

        const difficulty = progressForProblemType.difficulty;
        if(difficulty.toLowerCase() === minDifficulty.toLowerCase()) {
            nextProblemTypes.push(item);
        }
    }

    // sort it in ascending order using the formula CurDifPoints + attempted
    nextProblemTypes.sort((a,b) => {
        const aProblemType = convertStringToProblemType(a);
        const bProblemType = convertStringToProblemType(b);
        const aProgress = progress.get(aProblemType);
        const bProgress = progress.get(bProblemType);

        if(typeof aProgress === 'undefined' || typeof bProgress === 'undefined') {
            throw Boom.badData('progress can not be undefined');
        }

        return (aProgress.currentDifficultyPoints + aProgress.currentDifficultyAttempts) -
        (bProgress.currentDifficultyPoints + bProgress.currentDifficultyAttempts);
    });

    // last element is the minDifficulty
    nextProblemTypes.push(minDifficulty);

    return nextProblemTypes;
};

const getProblemsForClass = async (virtualClassroomUid: string) => {
    const classroom = await ClassroomModel.findById(virtualClassroomUid);

    if(!classroom) {
        throw Boom.notFound(`classroom not found with id ${virtualClassroomUid}`);
    }

    return classroom.problemsForToday;
};