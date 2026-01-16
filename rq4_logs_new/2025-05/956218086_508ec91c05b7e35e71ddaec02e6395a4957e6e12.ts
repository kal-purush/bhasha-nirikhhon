import fs from 'node:fs';
import path from 'node:path';
import { app } from 'electron';
import { createServiceLogger } from './logger';

const diagnosticsLogger = createServiceLogger('diagnostics');

export interface DiagnosticResult {
  name: string;
  status: 'ok' | 'warning' | 'error';
  message: string;
  details?: any;
}

export const runDiagnostics = async (): Promise<DiagnosticResult[]> => {
  const results: DiagnosticResult[] = [];

  // Check APP_ROOT environment variable
  results.push(checkAppRoot());

  // Check required binaries
  results.push(checkRedisBinary());
  results.push(checkQdrantBinary());
  results.push(checkPrismaBinary());

  // Check required directories
  results.push(checkUserDataDirectory());
  results.push(checkRendererDirectory());

  // Check Prisma schema
  results.push(checkPrismaSchema());

  // Check permissions
  results.push(await checkWritePermissions());

  // Log all results
  diagnosticsLogger.info('Diagnostic results:', results);

  return results;
};

const checkAppRoot = (): DiagnosticResult => {
  const appRoot = process.env.APP_ROOT;

  if (!appRoot) {
    return {
      name: 'APP_ROOT Environment Variable',
      status: 'error',
      message: 'APP_ROOT environment variable is not set',
    };
  }

  if (!fs.existsSync(appRoot)) {
    return {
      name: 'APP_ROOT Directory',
      status: 'error',
      message: `APP_ROOT directory does not exist: ${appRoot}`,
    };
  }

  return {
    name: 'APP_ROOT',
    status: 'ok',
    message: `APP_ROOT is set and exists: ${appRoot}`,
  };
};

const checkRedisBinary = (): DiagnosticResult => {
  const redisServerPath = path.join(process.env.APP_ROOT || '', 'bin', 'redis-server');

  if (!fs.existsSync(redisServerPath)) {
    return {
      name: 'Redis Binary',
      status: 'error',
      message: `Redis server binary not found at ${redisServerPath}`,
      details: { expectedPath: redisServerPath },
    };
  }

  try {
    const stats = fs.statSync(redisServerPath);
    if (!stats.isFile()) {
      return {
        name: 'Redis Binary',
        status: 'error',
        message: `Redis server path exists but is not a file: ${redisServerPath}`,
      };
    }

    // Check if executable (on Unix-like systems)
    if (process.platform !== 'win32') {
      const mode = stats.mode;
      const isExecutable = (mode & 0o111) !== 0;
      if (!isExecutable) {
        return {
          name: 'Redis Binary',
          status: 'warning',
          message: `Redis server binary may not be executable: ${redisServerPath}`,
          details: { mode: mode.toString(8) },
        };
      }
    }

    return {
      name: 'Redis Binary',
      status: 'ok',
      message: `Redis server binary found and appears valid: ${redisServerPath}`,
    };
  } catch (error) {
    return {
      name: 'Redis Binary',
      status: 'error',
      message: `Error checking Redis binary: ${error instanceof Error ? error.message : String(error)}`,
    };
  }
};

const checkQdrantBinary = (): DiagnosticResult => {
  const qdrantServerPath = path.join(process.env.APP_ROOT || '', 'bin', 'qdrant');

  if (!fs.existsSync(qdrantServerPath)) {
    return {
      name: 'Qdrant Binary',
      status: 'error',
      message: `Qdrant server binary not found at ${qdrantServerPath}`,
      details: { expectedPath: qdrantServerPath },
    };
  }

  try {
    const stats = fs.statSync(qdrantServerPath);
    if (!stats.isFile()) {
      return {
        name: 'Qdrant Binary',
        status: 'error',
        message: `Qdrant server path exists but is not a file: ${qdrantServerPath}`,
      };
    }

    // Check if executable (on Unix-like systems)
    if (process.platform !== 'win32') {
      const mode = stats.mode;
      const isExecutable = (mode & 0o111) !== 0;
      if (!isExecutable) {
        return {
          name: 'Qdrant Binary',
          status: 'warning',
          message: `Qdrant server binary may not be executable: ${qdrantServerPath}`,
          details: { mode: mode.toString(8) },
        };
      }
    }

    return {
      name: 'Qdrant Binary',
      status: 'ok',
      message: `Qdrant server binary found and appears valid: ${qdrantServerPath}`,
    };
  } catch (error) {
    return {
      name: 'Qdrant Binary',
      status: 'error',
      message: `Error checking Qdrant binary: ${error instanceof Error ? error.message : String(error)}`,
    };
  }
};

const checkPrismaBinary = (): DiagnosticResult => {
  const prismaPath = path.join(process.env.APP_ROOT || '', '..', 'node_modules', '.bin', 'prisma');

  if (!fs.existsSync(prismaPath)) {
    return {
      name: 'Prisma Binary',
      status: 'error',
      message: `Prisma binary not found at ${prismaPath}`,
      details: { expectedPath: prismaPath },
    };
  }

  return {
    name: 'Prisma Binary',
    status: 'ok',
    message: `Prisma binary found: ${prismaPath}`,
  };
};

const checkUserDataDirectory = (): DiagnosticResult => {
  try {
    const userDataPath = app.getPath('userData');

    if (!fs.existsSync(userDataPath)) {
      return {
        name: 'User Data Directory',
        status: 'warning',
        message: `User data directory does not exist but will be created: ${userDataPath}`,
      };
    }

    return {
      name: 'User Data Directory',
      status: 'ok',
      message: `User data directory exists: ${userDataPath}`,
    };
  } catch (error) {
    return {
      name: 'User Data Directory',
      status: 'error',
      message: `Error checking user data directory: ${error instanceof Error ? error.message : String(error)}`,
    };
  }
};

const checkRendererDirectory = (): DiagnosticResult => {
  const rendererDist = path.join(process.env.APP_ROOT || '', 'renderer');

  if (!fs.existsSync(rendererDist)) {
    return {
      name: 'Renderer Directory',
      status: 'error',
      message: `Renderer directory not found at ${rendererDist}`,
      details: { expectedPath: rendererDist },
    };
  }

  const indexPath = path.join(rendererDist, 'index.html');
  if (!fs.existsSync(indexPath)) {
    return {
      name: 'Renderer Directory',
      status: 'error',
      message: `Renderer index.html not found at ${indexPath}`,
      details: { expectedPath: indexPath },
    };
  }

  return {
    name: 'Renderer Directory',
    status: 'ok',
    message: `Renderer directory and index.html found: ${rendererDist}`,
  };
};

const checkPrismaSchema = (): DiagnosticResult => {
  const prismaSchemaPath = path.join(process.env.APP_ROOT || '', 'prisma', 'sqlite-schema.prisma');

  if (!fs.existsSync(prismaSchemaPath)) {
    return {
      name: 'Prisma Schema',
      status: 'error',
      message: `Prisma schema not found at ${prismaSchemaPath}`,
      details: { expectedPath: prismaSchemaPath },
    };
  }

  return {
    name: 'Prisma Schema',
    status: 'ok',
    message: `Prisma schema found: ${prismaSchemaPath}`,
  };
};

const checkWritePermissions = async (): Promise<DiagnosticResult> => {
  try {
    const userDataPath = app.getPath('userData');
    const tempPath = app.getPath('temp');

    // Test write permission to user data directory
    const testFile = path.join(userDataPath, 'write-test.tmp');
    try {
      // Ensure directory exists
      if (!fs.existsSync(userDataPath)) {
        fs.mkdirSync(userDataPath, { recursive: true });
      }

      fs.writeFileSync(testFile, 'test');
      fs.unlinkSync(testFile);
    } catch (error) {
      return {
        name: 'Write Permissions',
        status: 'error',
        message: `Cannot write to user data directory: ${userDataPath}`,
        details: { error: error instanceof Error ? error.message : String(error) },
      };
    }

    // Test write permission to temp directory
    const tempTestFile = path.join(tempPath, 'write-test.tmp');
    try {
      fs.writeFileSync(tempTestFile, 'test');
      fs.unlinkSync(tempTestFile);
    } catch (error) {
      return {
        name: 'Write Permissions',
        status: 'warning',
        message: `Cannot write to temp directory: ${tempPath}`,
        details: { error: error instanceof Error ? error.message : String(error) },
      };
    }

    return {
      name: 'Write Permissions',
      status: 'ok',
      message: 'Write permissions verified for user data and temp directories',
    };
  } catch (error) {
    return {
      name: 'Write Permissions',
      status: 'error',
      message: `Error checking write permissions: ${error instanceof Error ? error.message : String(error)}`,
    };
  }
};

export const logDiagnosticsSummary = (results: DiagnosticResult[]) => {
  const errors = results.filter((r) => r.status === 'error');
  const warnings = results.filter((r) => r.status === 'warning');
  const ok = results.filter((r) => r.status === 'ok');

  diagnosticsLogger.info(
    `Diagnostics Summary: ${ok.length} OK, ${warnings.length} warnings, ${errors.length} errors`,
  );

  if (errors.length > 0) {
    diagnosticsLogger.error(
      'Critical issues found:',
      errors.map((e) => e.message),
    );
  }

  if (warnings.length > 0) {
    diagnosticsLogger.warn(
      'Warnings found:',
      warnings.map((w) => w.message),
    );
  }

  return {
    hasErrors: errors.length > 0,
    hasWarnings: warnings.length > 0,
    summary: `${ok.length} OK, ${warnings.length} warnings, ${errors.length} errors`,
  };
};