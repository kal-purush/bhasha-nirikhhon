'use server';

import { randomInt } from 'crypto';
import { Quiz } from '../../model/interface/quiz';
import { findQuiz } from './api.backend';
import quizContextScheduler, { QuizContext } from './api.quiz.scheduler';
import { FindQuizResult } from './dto';
import { serverLogger } from '../../util/logger';
import { Painting } from '../../model/interface/painting';

export interface QuizStatus {
    context: QuizContext;
    currentIdx: number;
    endIdx: number;
}

export interface ResponseQuizDTO {
    quiz: Quiz;
    status: QuizStatus;
}

export async function getQuizIDByContext(status?: QuizStatus): Promise<ResponseQuizDTO> {
    /*TODO
    - cache 로직 적용하기.
        - 매번 backend에서 Quiz[]를 가져오는 건, 변형된 Quiz[]를 가져올 수 있다.
        - quizContextScheduler 모듈에 기능을 추가하거나, nextjs cache 기능을 활용한다.
    */
    const INIT_IDX = -1;
    if (!status || (status && status.currentIdx == status.endIdx)) {
        const context = await quizContextScheduler.scheduleContext();
        status = {
            context,
            currentIdx: INIT_IDX,
            endIdx: INIT_IDX,
        };
    }

    try {
        const result: FindQuizResult = await findQuiz(
            [status.context.artist || ''],
            [status.context.tag || ''],
            [status.context.style || ''],
            status.context.page,
        );
        const quizList: Quiz[] = result.data;

        status.endIdx = quizList.length - 1;

        if (status.currentIdx === INIT_IDX) {
            status.currentIdx = randomInt(quizList.length);
        } else {
            status.currentIdx = (status.currentIdx + 1) % quizList.length;
        }

        return {
            quiz: quizList[status.currentIdx],
            status,
        };
    } catch (e: unknown) {
        serverLogger.error(`fail getQuizIDByContext(). ${JSON.stringify(e)}`);
        /*TODO
        - Q. next js에서 server action이 던진 익셉션을 클라이언트에서 어떻게 처리되는가?
        */
        throw e;
    }
}

export function addQuizContextByPainting(painting: Painting): Promise<boolean> {
    const artistName: string = painting.artist.name;
    const context: QuizContext = {
        artist: artistName,
        page: 0,
    };

    return quizContextScheduler.requestAddContext([context]);
}

export function updateFixedContexts(quizContexts: QuizContext[]): Promise<boolean> {
    return quizContextScheduler.requestUpdateFixedQuiz(quizContexts);
}