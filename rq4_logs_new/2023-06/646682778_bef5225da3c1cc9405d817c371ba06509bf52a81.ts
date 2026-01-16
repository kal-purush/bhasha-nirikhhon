import path from 'path'
import { createLogger, format, transports } from 'winston'
const { combine, timestamp, label, printf } = format

const myFormat = printf(({ level, message, label, timestamp }) => {
  const date = new Date(timestamp)
  const hours = date.getHours()
  const minutes = date.getMinutes()
  const seconds = date.getSeconds()

  return `{
    message: ${message},
    level: ${level},
    Project: ${label},
    Date: ${date.toDateString()} ,
    time: ${hours}:${minutes}:${seconds}
  }`
})

const infoLogger = createLogger({
  level: 'info',
  format: combine(label({ label: 'UMS' }), timestamp(), myFormat),
  defaultMeta: { service: 'user-service' },
  transports: [
    new transports.Console(),
    new transports.File({
      filename: path.join(process.cwd(), 'logs', 'winston', 'success.log'),
      level: 'info',
    }),
  ],
})

const errorLogger = createLogger({
  level: 'error',
  format: combine(label({ label: 'UMS' }), timestamp(), myFormat),
  defaultMeta: { service: 'user-service' },
  transports: [
    new transports.Console(),
    new transports.File({
      filename: path.join(process.cwd(), 'logs', 'winston', 'error.log'),
      level: 'error',
    }),
  ],
})

export { infoLogger, errorLogger }

// logs/winston/success.log
// logs/winston/error.log