import path from 'node:path';
import { app } from 'electron';
import { existsSync } from 'node:fs';
import { fork } from 'node:child_process';
import log from 'electron-log';

const platformToExecutables = {
  win32: {
    schemaEngine: 'node_modules/@prisma/engines/schema-engine-windows.exe',
    queryEngine: 'node_modules/@prisma/engines/query_engine-windows.dll.node',
  },
  linux: {
    schemaEngine: 'node_modules/@prisma/engines/schema-engine-debian-openssl-1.1.x',
    queryEngine: 'node_modules/@prisma/engines/libquery_engine-debian-openssl-1.1.x.so.node',
  },
  darwin: {
    schemaEngine: 'node_modules/@prisma/engines/schema-engine-darwin-arm64',
    queryEngine: 'node_modules/@prisma/engines/libquery_engine-darwin-arm64.dylib.node',
  },
};

export async function runPrismaCommand({ command, dbUrl }) {
  const asarUnpackedRoot = app.getAppPath().replace('app.asar', 'app.asar.unpacked');

  const qePath = path.join(asarUnpackedRoot, platformToExecutables[process.platform].queryEngine);
  log.info('Query engine path', qePath);

  if (!existsSync(qePath)) {
    throw new Error(`Query engine path does not exist: ${qePath}`);
  }

  const sePath = path.join(asarUnpackedRoot, platformToExecutables[process.platform].schemaEngine);
  log.info('Schema engine path', sePath);

  if (!existsSync(sePath)) {
    throw new Error(`Schema engine path does not exist: ${sePath}`);
  }

  try {
    const exitCode = await new Promise((resolve, _) => {
      const prismaPath = path.resolve(app.getAppPath(), 'node_modules/prisma/build/index.js');
      log.info('Prisma path', prismaPath);

      const child = fork(prismaPath, command, {
        env: {
          ...process.env,
          DATABASE_URL: dbUrl,
          PRISMA_SCHEMA_ENGINE_BINARY: sePath,
          PRISMA_QUERY_ENGINE_LIBRARY: qePath,
        },
        stdio: 'inherit',
      });

      child.on('error', (err) => {
        log.error('Child process got error:', err);
      });

      child.on('close', (code) => {
        resolve(code);
      });
    });

    if (exitCode !== 0) {
      const errorMsg = `command '${command.join(' ')}' failed with exit code ${exitCode}`;
      throw new Error(errorMsg);
    }

    return exitCode;
  } catch (e) {
    log.error(e);
    throw e;
  }
}