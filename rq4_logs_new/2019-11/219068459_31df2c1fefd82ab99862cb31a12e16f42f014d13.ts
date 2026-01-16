import { CognitoIdentity, CognitoIdentityServiceProvider } from "aws-sdk";
import { USER_POOL_ID, ID_POOL_ID } from "./env";

(async () => {
  const cognitoProvider = new CognitoIdentityServiceProvider();

  await cognitoProvider
    .deleteUserPool({ UserPoolId: USER_POOL_ID })
    .promise()
    .catch(e => {
      console.log(e);
    });

  const cognitoIdentity = new CognitoIdentity();

  await cognitoIdentity
    .deleteIdentityPool({
      IdentityPoolId: ID_POOL_ID
    })
    .promise()
    .catch(e => {
      console.log(e);
    });

  console.log("UserPool and Identity Pool were deleted!");
})();