//fichero de configuracion
import bodyParser from 'body-parser' //se encarga de extraer los datos y de ponerlos en req.body
import logger from 'morgan' //simplifica los procesos de loggin en la aplicacion
import cors from 'cors' //permite el manejo de recursos desde un origen diferente o cruzado
import { config } from 'dotenv' //permite la carga de variables desde un fichero .env

const SETTINGS = config()

export default app => {
  app.disable('x-powered-by')

  app.set('env', SETTINGS.parsed.ENV)

  app.set('config', SETTINGS.parsed)
  app.locals.env = app.get('env')
  app.locals.config = app.get('config')

  app.use(logger('combined'))

  app.use(bodyParser.json())
  app.use(bodyParser.urlencoded({ extended: false }))

  app.use(cors())
}