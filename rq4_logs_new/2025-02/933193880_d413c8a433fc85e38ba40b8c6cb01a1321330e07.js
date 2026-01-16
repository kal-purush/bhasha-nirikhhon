if (process.env.NODE_ENV !== 'production') require('dotenv').config() // eslint-disable-line

module.exports = {
  cookieSecret: process.env.COOKIE_SECRET,
  env: process.env.NODE_ENV,
  mongodbUri: process.env.MIPIE_MONGODB_URI,
  port: process.env.MIPIE_PORT,
  headerAuthKey: process.env.MIPIE_HEADER_AUTH_KEY,
  ykError: (msg = 'Something went wrong!') => {
    const error = new Error(msg);
    error.name = 'ykE/rror';
    error.code = 999;
    return error;
  },
  msg91AuthKey: process.env.MIPIE_MSG91_AUTH_KEY,
  msg91SenderId: process.env.MIPIE_MSG91_SENDER_ID,
  jwtSecret: process.env.MIPIE_JWT_SECRET,
  mimetypes: 'jpg png gif jpeg bmp pdf doc docx xlsx xls zip',
  accessKeyId: process.env.AWS_ACCESS_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  bucketName: process.env.AWS_BUCKET_NAME,
  region: process.env.AWS_REGION_NAME,
  fileURL: process.env.MIPIE_BUCKET_URL,
  authKey: process.env.AUTH_KEY,
  templateId:process.env.TEMPLATE_ID,
  msg91Url: process.env.MYPIE_MSG91_URL,
  bucketURL:process.env.MIPIE_BUCKET_URL,
  msg91WelcomeTemplate:process.env.MIPIE_MSG91_WELCOME_TEMPLATE,
  msg91SmsUrl : process.env.MIPIE_MSG91_SMS_URL,
  msg91ShortlistedTemplate:process.env.MIPIE_MSG91_SHORTLISTED,
  msg91Rejected : process.env.MIPIE_MSG91_REJECTED,
  msg91Hired : process.env.MIPIE_MSG91_HIRED,
  msg91InterviewScheduled : process.env.MIPIE_MSG91_INTERVIEW_SCHEDULED,
  msg91OnHoldTemplate : process.env.MIPIE_MSG91_ON_HOLD,
  msg91ProfileStrengthening : process.env.MIPIE_MSG91_PROFILE_STRENGTHENING,
  extraEdgeUrl : process.env.EXTRA_EDGE_API_URL,
  extraEdgeAuthToken : process.env.EXTRA_EDGE_AUTH_TOKEN,
  fbConversionPixelId : process.env.FB_CONVERSION_PIXEL_ID,
  fbConversionAccessToken : process.env.FB_CONVERSION_ACCESS_TOKEN,
  translateProjectId: process.env.MIPIE_PROJECTID,
  translateKey: process.env.MIPIE_TRANSLATE_KEY,
  baseUrl: process.env.BASE_URL,
  blackListIps: process.env.BLACK_LIST_IPS,
  chat_service_api: process.env.MIPIE_CHAT_SERVICE_KEY,
  sheetId: process.env.sheetId
};