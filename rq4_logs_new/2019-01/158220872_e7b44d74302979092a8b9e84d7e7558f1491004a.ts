
import { KeycloakConfig } from 'keycloak-angular';


const base_url = 'https://dev.shikshalokam.org';
const apibaseurl = ' https://dev.shikshalokam.org/assessment/api/v1/';
const downloadReportHeaderLabel = 'internal-access-token';
const downloadReportHeaderValue = 'Fgn1xT7pmCK9PSxVt7yr';

let keycloakConfig: KeycloakConfig = {
  url: base_url + '/auth',
  realm: 'sunbird',
  clientId: 'sl-ionic-connect',
  "credentials": {
     "secret": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJkYTJiMTA5MWVlMDE0MDQ3OTdhYjRjZDI3ODJmYTFkZCJ9.olC-mJ9JVqeeIf-eyBVYciPIIsqDm46XHbKuO1GgNG0"
   } 
};

export const environment = {
  production: false,
  keycloak: keycloakConfig,
  base_url: base_url,
  apibaseurl :apibaseurl,
  downloadReportHeaderLabel:downloadReportHeaderLabel,
  downloadReportHeaderValue:downloadReportHeaderValue,
  
};