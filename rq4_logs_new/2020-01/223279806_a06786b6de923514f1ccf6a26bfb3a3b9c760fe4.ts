import { getConnection } from 'typeorm';

import all from './all';
import User from '../../../db/entity/user';
import { createUser, deleteUser } from '../../../../testUtils/user';
import { createProject } from '../../../../testUtils/project';

describe('The user get all own projects', () => {
  let user: User;

  beforeAll(async () => {
    await getConnection().connect();
    user = await createUser('testAllProject', 'testAllMail@mail.gd', 'pass');
    await createProject(user, 'test');
    await createProject(user, 'test1');
  });

  test('All own projects to be send', async () => {
    const projects = await all(user.id);

    expect(projects[0].name).toEqual('test');
    expect(projects[1].name).toEqual('test1');
  });

  afterAll(async () => {
    await deleteUser(user);
    await getConnection().close();
  });
});