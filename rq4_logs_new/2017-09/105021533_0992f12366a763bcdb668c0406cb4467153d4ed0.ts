declare module config {
  interface Environments {
    development: Configuration
    test: Configuration
    production: Configuration
  }

  interface Configuration {
    database: DatabaseConfiguration
    server: ServerConfiguration
    logger: LoggerConfiguration
  }

  interface DatabaseConfiguration {
    dialect: string
    connection: string
    modelPaths: string[]
  }

  interface ServerConfiguration {
    host: string
    port: string
    graphiql: boolean
  }

  interface LoggerConfiguration {
    host?: string
    port?: string
    file?: ConsoleLoggerConfiguration
    console: ConsoleLoggerConfiguration
    debug: string
    context?: string
  }

  interface LogHandlers {
    debug: (message: string, ...args: any[]) => any
    verbose?: (message: string, ...args: any[]) => any
    silly?: (message: string, ...args: any[]) => any
    info: (message: string, ...args: any[]) => any
    warn: (message: string, ...args: any[]) => any
    error: (message: string, ...args: any[]) => any
  }

  interface ConsoleLoggerConfiguration {
    level: string
  }
}