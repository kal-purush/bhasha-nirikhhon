import { GenSSL } from "../generated-typings";
const exec = require('child_process').execSync;
import { checkJWT } from "../utils/checkauth";

const genSSL: GenSSL = (jwToken, userName, sessionType, sslCommonName, sslCountry, sslLocation, sslOrg) => {
  return new Promise( async(resolve, reject) => {
    const checkToken: any = await checkJWT( jwToken, userName, sessionType );
    if( checkToken.name ) return resolve("authError");

    const buildSSLCertRes: any = buildSSLcert(sslCommonName, sslCountry, sslLocation, sslOrg);
    resolve(buildSSLCertRes);
    return;
  });
};

export const buildSSLcert = async ( sslCommonName: string, sslCountry: string, sslLocation: string, sslOrg: string ) => {
  const genSSLcmd: string = `openssl req -x509 -sha256 -days 356 -nodes -newkey rsa:2048 -subj "/CN=${sslCommonName}/C=${sslCountry}/L=${sslLocation}/0=${sslOrg}" -keyout cert/server.key -out cert/server.cert`
  console.log(await exec( genSSLcmd , { "encoding":"utf8" } ));
  return("ok");
};

export default genSSL;