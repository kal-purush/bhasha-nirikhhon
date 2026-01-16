import { createParamDecorator, ExecutionContext } from '@nestjs/common';
import { Request } from 'express';

/**
 * Información de auditoría para ser registrada
 */
export interface AuditInfo {
  user: {
    id: string;
    email?: string;
  };
  action: string;
  resourceType: string;
  resourceId: string;
  ipAddress: string;
  userAgent: string;
  additionalData?: Record<string, any>;
}

/**
 * Decorador para auditar acceso a datos sensibles
 * @param actionType Tipo de acción (READ, UPDATE, DELETE, etc.)
 * @param resourceType Tipo de recurso al que se accede
 */
export const AuditAccess = (actionType: string, resourceType: string) => {
  return (
    target: any,
    propertyKey: string,
    descriptor: PropertyDescriptor,
  ) => {
    const originalMethod = descriptor.value;

    descriptor.value = async function (...args: any[]) {
      try {
        // Obtener el contexto de ejecución
        const req = args.find((arg) => arg?.request || arg?.req)?.request || 
                    args.find((arg) => arg instanceof Request);
        
        if (!req || !req.user) {
          console.warn(`Auditoría: No se pudo obtener el usuario para ${actionType} en ${resourceType}`);
          return originalMethod.apply(this, args);
        }
        
        // Obtener ID del recurso desde los parámetros (si existe)
        let resourceId = 'unknown';
        for (const arg of args) {
          if (arg && (arg.id || arg.resourceId || arg.entityId)) {
            resourceId = arg.id || arg.resourceId || arg.entityId;
            break;
          }
        }
        
        // Construir información de auditoría
        const auditInfo: AuditInfo = {
          user: {
            id: req.user.id || req.user.sub,
            email: req.user.email,
          },
          action: actionType,
          resourceType,
          resourceId,
          ipAddress: req.ip || req.connection?.remoteAddress || 'unknown',
          userAgent: req.headers?.['user-agent'] || 'unknown',
          additionalData: {},
        };
        
        // Si hay un servicio de auditoría inyectado en esta clase, usarlo
        if (this.auditService && typeof this.auditService.logAccess === 'function') {
          this.auditService.logAccess(auditInfo);
        } else {
          // Fallback a console.log
          console.log(
            `[AUDIT] ${new Date().toISOString()} | ${auditInfo.user.id} | ${auditInfo.action} | ${auditInfo.resourceType} | ${auditInfo.resourceId} | ${auditInfo.ipAddress}`,
          );
        }
        
        // Ejecutar el método original
        return await originalMethod.apply(this, args);
      } catch (error) {
        // Registrar el error y re-lanzarlo
        console.error(`Error en auditoría para ${actionType} en ${resourceType}:`, error);
        throw error;
      }
    };
    
    return descriptor;
  };
}; 