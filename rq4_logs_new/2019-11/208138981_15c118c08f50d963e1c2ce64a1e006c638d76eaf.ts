export interface IStudentStatistic {
    numDailyAttempts: number;
    numDailyCorrectAnswers: number;
    totals: {
        [key: string]: {
            totalAttempts: number;
            totalCorrectAnswers: number;
        };
    };
}

export interface IClassroomStatistic {
    studentsCompleted: number;
    numDailyAttempts: number;
    numDailyCorrectAnswers: number;
    problemTypeStats: {
        [key: string]: {
            totalAttempts: number;
            totalCorrectAnswers: number;
        };
    };
    allStudents: { [key: string]: IStudentStatistic };
}