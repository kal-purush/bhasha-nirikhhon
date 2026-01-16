import { Helper } from './helper/common';
import { Setup } from './helper/setup';

describe('design', () => {
  let helper: Helper;

  beforeAll(async () => {
    const setup = new Setup();
    helper = await setup.setup();
  });

  afterAll(async () => {
    await helper.close();
  });

  afterEach(async () => {
    await helper.trancateAll();
  });

  it('designの詳細を返す', async () => {
    await helper.insertDesign('title');
    const res = await helper.get('/designs/1', {}, true);
    expect(res.body).toEqual({ id: 1 });
    expect(res.status).toEqual(200);
  });

  it('該当するdesignが無い', async () => {
    const res = await helper.get('/designs/1', {}, true);
    expect(res.body).toEqual({
      message: 'not found design: 1',
      statusCode: 403,
    });
    expect(res.status).toEqual(403);
  });

  it('designの一覧を返す', async () => {
    await helper.insertDesign('title1');
    await helper.insertDesign('title2');
    const res = await helper.get('/designs', {}, true);
    expect(res.body).toEqual([
      { id: 1, name: 'title1' },
      { id: 2, name: 'title2' },
    ]);
    expect(res.status).toEqual(200);
  });

  it('designの作成をする', async () => {
    const res = await helper.post('/designs/create', { title: 'title1' }, true);
    expect(res.status).toEqual(201);
    // TODO: レコードのチェックもする
  });
});