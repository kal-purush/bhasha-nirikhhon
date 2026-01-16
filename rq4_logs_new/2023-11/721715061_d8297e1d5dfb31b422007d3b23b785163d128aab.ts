import { registerAs } from '@nestjs/config'
import * as process from 'process'

export default registerAs('auth', () => ({
  secret: process.env.JWT_SECRET,
}))