import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as net from 'net';
import * as os from 'os';

interface LogstashConfig {
  enabled: boolean;
  host: string;
  port: number;
  appName: string;
  environment: string;
}

@Injectable()
export class LogstashService implements OnModuleInit, OnModuleDestroy {
  private client: net.Socket | null = null;
  private config: LogstashConfig;
  private isConnected = false;
  private reconnectTimeout: NodeJS.Timeout | null = null;
  private readonly reconnectInterval = 5000; // 5 segundos
  private readonly logger = new Logger(LogstashService.name);
  private readonly maxReconnectAttempts = 10;
  private reconnectAttempts = 0;
  private readonly messageQueue: string[] = [];
  private readonly maxQueueSize = 1000;
  private readonly hostname = os.hostname();

  constructor(private readonly configService: ConfigService) {
    const environment = this.configService.get<string>('NODE_ENV', 'development');
    
    // Usar localhost en entorno de desarrollo cuando se ejecuta fuera de Docker
    const host = environment === 'development' && !process.env.DOCKER_CONTAINER
      ? 'localhost'
      : this.configService.get<string>('LOGSTASH_HOST', 'logstash');
    
    this.config = {
      enabled: this.configService.get<string>('ELK_ENABLED', 'false') === 'true',
      host: host,
      port: parseInt(this.configService.get<string>('LOGSTASH_PORT', '50000'), 10),
      appName: this.configService.get<string>('APP_NAME', 'novack-backend'),
      environment: environment,
    };
    
    this.logger.log(`Configuración de Logstash: host=${this.config.host}, port=${this.config.port}, enabled=${this.config.enabled}`);
  }

  async onModuleInit() {
    if (this.config.enabled) {
      this.connect();
    } else {
      this.logger.log('Logstash está desactivado en la configuración');
    }
  }

  onModuleDestroy() {
    this.disconnect();
  }

  private connect() {
    if (this.isConnected || this.client) {
      return;
    }

    this.client = new net.Socket();

    this.client.connect(this.config.port, this.config.host, () => {
      this.isConnected = true;
      this.reconnectAttempts = 0;
      this.logger.log(`Conectado a Logstash en ${this.config.host}:${this.config.port}`);
      
      // Procesar mensajes en cola
      this.processQueue();
    });

    this.client.on('error', (err) => {
      this.logger.error(`Error en la conexión con Logstash: ${err.message}`);
      this.handleDisconnect();
    });

    this.client.on('close', () => {
      this.logger.warn('Conexión con Logstash cerrada');
      this.handleDisconnect();
    });
  }

  private handleDisconnect() {
    this.isConnected = false;
    this.client = null;

    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
    }

    if (this.reconnectAttempts < this.maxReconnectAttempts) {
      this.reconnectAttempts++;
      this.logger.log(`Intentando reconectar a Logstash (intento ${this.reconnectAttempts}/${this.maxReconnectAttempts})`);
      this.reconnectTimeout = setTimeout(() => this.connect(), this.reconnectInterval);
    } else {
      this.logger.error(`Se alcanzó el número máximo de intentos de reconexión (${this.maxReconnectAttempts})`);
    }
  }

  private disconnect() {
    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
      this.reconnectTimeout = null;
    }

    if (this.client) {
      this.client.end();
      this.client.destroy();
      this.client = null;
    }

    this.isConnected = false;
  }

  private addToQueue(message: string) {
    // Limitar el tamaño de la cola para evitar problemas de memoria
    if (this.messageQueue.length >= this.maxQueueSize) {
      this.messageQueue.shift(); // Eliminar el mensaje más antiguo
    }
    
    this.messageQueue.push(message);
  }

  private processQueue() {
    if (!this.isConnected || !this.client) {
      return;
    }

    while (this.messageQueue.length > 0 && this.isConnected) {
      const message = this.messageQueue.shift();
      if (message) {
        try {
          this.client.write(message + '\n');
        } catch (error) {
          this.logger.error(`Error al enviar mensaje desde la cola: ${error.message}`);
          // Volver a poner el mensaje en la cola si hay error
          this.addToQueue(message);
          break;
        }
      }
    }
  }

  send(logData: Record<string, any>): void {
    if (!this.config.enabled) {
      return;
    }

    // Añadir metadata estándar
    const enrichedData = {
      ...logData,
      application: this.config.appName,
      environment: this.config.environment,
      hostname: this.hostname,
      pid: process.pid,
      timestamp: new Date().toISOString(),
    };

    const logMessage = JSON.stringify(enrichedData);

    if (this.isConnected && this.client) {
      try {
        this.client.write(logMessage + '\n');
      } catch (error) {
        this.logger.error(`Error al enviar log a Logstash: ${error.message}`);
        this.addToQueue(logMessage);
      }
    } else {
      this.addToQueue(logMessage);
      if (!this.isConnected && this.reconnectAttempts < this.maxReconnectAttempts) {
        this.connect();
      }
    }
  }

  // Método para registro de logs de información
  info(message: string, context?: string, correlationId?: string): void {
    this.send({
      level: 'info',
      message,
      context: context || 'Application',
      correlationId: correlationId || 'no-correlation-id',
    });
  }

  // Método para registro de logs de error
  error(message: string, trace?: string, context?: string, correlationId?: string): void {
    this.send({
      level: 'error',
      message,
      trace,
      context: context || 'Application',
      correlationId: correlationId || 'no-correlation-id',
    });
  }

  // Método para registro de logs de advertencia
  warn(message: string, context?: string, correlationId?: string): void {
    this.send({
      level: 'warn',
      message,
      context: context || 'Application',
      correlationId: correlationId || 'no-correlation-id',
    });
  }

  // Método para registro de logs de depuración
  debug(message: string, context?: string, correlationId?: string): void {
    this.send({
      level: 'debug',
      message,
      context: context || 'Application',
      correlationId: correlationId || 'no-correlation-id',
    });
  }

  // Método para solicitar estado de la conexión
  isLogstashConnected(): boolean {
    return this.isConnected;
  }
} 