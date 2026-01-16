import { createConnection, getConnection, getConnectionOptions } from 'typeorm';

export default async () => {
  let name = 'default';
  if (process.env.NODE_ENV === 'test') {
    name = process.env.NODE_ENV;
  }

  const connectionOptions = await getConnectionOptions(name);
  // default 선언 안되어 있으면 무조건 에러 발생
  await createConnection({ ...connectionOptions, name: 'default' })
    .then(() => {
      console.log('Database Connected :)');
    })
    .catch(error => console.log(error));
};

export const closeDatabaseConn = async () => {
  getConnection().close();
};