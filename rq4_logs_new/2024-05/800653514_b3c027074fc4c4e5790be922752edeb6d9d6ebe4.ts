import { registrationCss } from '../css/registration';
import { baseProcessor } from './base-processor';

export const registrationAdminTemplateDataBind = (
  templateBody: string,
  data: {
    name: string;
    link: string;
    generatedPassword: string;
  },
): string => {
  let template = baseProcessor(templateBody);
  template = template.replace('{{CSS}}', registrationCss);
  template = template.replace(/{PASSWORD_ADMIN}}/, data.generatedPassword)
  template = template.replace(/{{userName}}/g, data.name);
  template = template.replace(/{{link}}/g, data.link);

  return template;
};