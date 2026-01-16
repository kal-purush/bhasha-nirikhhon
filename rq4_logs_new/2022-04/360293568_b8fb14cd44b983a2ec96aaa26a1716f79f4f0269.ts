import IComment from '../components/interfaces/IComment';
import Comment from './Comment';
import nock from 'nock';

const testURL: string = process.env.REACT_APP_SERVER_URL as string;

test('ensure that getAll occurs without technical errors', async () => {
    const scope = nock(testURL)
        .get('/safeplace/comment')
        .reply(200, [], {
            'Access-Control-Allow-Origin': '*'
        });

    const response = await Comment.getAll("");
    expect(response.status).toBe(200);
    scope.done();
});

test('ensure that get occurs without technical errors', async () => {
    const scope = nock(testURL)
        .get('/safeplace/comment/1')
        .reply(200, {}, {
            'Access-Control-Allow-Origin': '*'
        });

    const response = await Comment.get("1", "");
    expect(response.status).toBe(200);
    scope.done();
});

test('ensure that getBest occurs without technical errors', async () => {
    const scope = nock(testURL)
        .get('/safeplace/comment/best/5')
        .reply(200, [], {
            'Access-Control-Allow-Origin': '*'
        });

    const response = await Comment.getBest(5, "");
    expect(response.status).toBe(200);
    scope.done();
});

test('ensure that getBestSafeplace occurs without technical errors', async () => {
    const scope = nock(testURL)
        .get('/safeplace/comment/best/1/5')
        .reply(200, [], {
            'Access-Control-Allow-Origin': '*'
        });

    const response = await Comment.getBestSafeplace("1", 5, "");
    expect(response.status).toBe(200);
    scope.done();
});

test('ensure that getWorst occurs without technical errors', async () => {
    const scope = nock(testURL)
        .get('/safeplace/comment/worst/5')
        .reply(200, [], {
            'Access-Control-Allow-Origin': '*'
        });

    const response = await Comment.getWorst(5, "");
    expect(response.status).toBe(200);
    scope.done();
});

test('ensure that getWorstSafeplace occurs without technical errors', async () => {
    const scope = nock(testURL)
        .get('/safeplace/comment/worst/1/5')
        .reply(200, [], {
            'Access-Control-Allow-Origin': '*'
        });

    const response = await Comment.getWorstSafeplace("1", 5, "");
    expect(response.status).toBe(200);
    scope.done();
});

test('ensure that create occurs without technical errors', async () => {
    const scopeCreate = nock(testURL)
        .post('/safeplace/comment')
        .reply(200, {}, {
            'Access-Control-Allow-Origin': '*'
        });

    const comment: IComment = {
        id: "",
        userId: "",
        safeplaceId: "",
        comment: "",
        grade: 0
    };

    const response = await Comment.create(comment, "");
    expect(response.status).toBe(200);
    scopeCreate.done();
});

test('ensure that update occurs without technical errors', async () => {
    const scopeOptions = nock(testURL)
        .options('/safeplace/comment/1')
        .reply(200, {}, { 'Access-Control-Allow-Origin': '*' });
    const scopeUpdate = nock(testURL)
        .put('/safeplace/comment/1')
        .reply(200, {}, {
            'Access-Control-Allow-Origin': '*'
        });

    const comment: IComment = {
        id: "",
        userId: "",
        safeplaceId: "",
        comment: "",
        grade: 0
    };

    const response = await Comment.update("1", comment, "");
    expect(response.status).toBe(200);
    scopeOptions.done();
    scopeUpdate.done();
});

test('ensure that delete occurs without technical errors', async () => {
    const scopeOptions = nock(testURL)
        .options('/safeplace/comment/1')
        .reply(200, {}, { 'Access-Control-Allow-Origin': '*' });
    const scopeDelete = nock(testURL)
        .delete('/safeplace/comment/1')
        .reply(200, {}, {
            'Access-Control-Allow-Origin': '*'
        });

    const response = await Comment.delete("1", "");
    expect(response.status).toBe(200);
    scopeOptions.done();
    scopeDelete.done();
});