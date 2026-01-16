import Joi from 'joi';

type Env = {
  PORT: string;
  NODE_ENV: string;
  RDS_HOST: string;
  RDS_PORT: string;
  RDS_USERNAME: string;
  RDS_PASSWORD: string;
  RDS_DATABASE: string;
  KAKAO_REST_API_KEY: string;
  KAKAO_REDIRECT_URI: string;
  JWT_SECRET: string;
};

const { value: env, error } = Joi.object<Env>()
  .keys({
    PORT: Joi.string(),
    NODE_ENV: Joi.string(),
    RDS_HOST: Joi.string(),
    RDS_PORT: Joi.string(),
    RDS_USERNAME: Joi.string(),
    RDS_PASSWORD: Joi.string(),
    RDS_DATABASE: Joi.string(),
    KAKAO_REST_API_KEY: Joi.string(),
    KAKAO_REDIRECT_URI: Joi.string(),
    JWT_SECRET: Joi.string(),
  })
  .unknown()
  .validate(process.env);

if (error) throw error;

export default env as Env;