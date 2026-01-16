// auditMiddleware.js
//@ts-nocheck
const db = require('./dbClient'); // Import your db client instance

export const auditMiddleware = async (params, next) => {
  const before = params.action === 'update' || params.action === 'delete'
    ? await db[params.model].findUnique({ where: params.args.where })
    : null;

  const result = await next(params);

  const after = params.action === 'create' || params.action === 'update'
    ? await db[params.model].findUnique({ where: params.args.where })
    : null;

  if (['create', 'update', 'delete'].includes(params.action)) {
    const auditEntry = {
      action: params.action,
      tableName: params.model,
      recordId: params.args.where?.id || result.id,
      oldValue: before ? before : null,
      newValue: after ? after : result,
      timestamp: new Date(),
      userId: params.args.userId || null // Ensure userId is passed to args
    };
    await db.audit.create({ data: auditEntry });
  }

  return result;
};
