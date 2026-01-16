import { Injectable, NestMiddleware } from '@nestjs/common';
import { Request, Response, NextFunction } from 'express';
import { StructuredLoggerService, LogContext } from './structured-logger.service';

@Injectable()
export class CorrelationIdMiddleware implements NestMiddleware {
  use(req: Request, res: Response, next: NextFunction): void {
    // Obtener correlationId del header o crear uno nuevo
    const correlationId = 
      req.headers['x-correlation-id'] as string || 
      StructuredLoggerService.createCorrelationId();
    
    // Establecer el correlationId en los headers de respuesta
    res.setHeader('x-correlation-id', correlationId);
    
    // Crear contexto de log con información de la solicitud
    const logContext: LogContext = {
      correlationId,
      requestPath: req.path,
      method: req.method,
      userAgent: req.headers['user-agent'],
      ip: req.ip,
    };

    // Si hay un usuario autenticado (asumiendo que está disponible en req.user)
    if (req.user) {
      logContext.userId = (req.user as any).id || (req.user as any).userId;
    }

    // Ejecutar el middleware en el contexto del correlationId
    StructuredLoggerService.getContextStorage().run(logContext, () => {
      next();
    });
  }
} 