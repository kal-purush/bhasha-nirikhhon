const axios = require('axios');
const https = require('https');
const fs = require('fs');
const path = require('path');

const instance = axios.create({
  httpsAgent: new https.Agent({
    rejectUnauthorized: false,
  }),
});


/* The code below will create operation names.
Instead of `/api/User/GetList` you'll get `UserGetList` type
that you can use anywhere */

function addOperationIdsToSchema(schema) {
  const data = schema;

  Object.keys(data.paths).forEach((endpointPath) => {
    const operations = Object.keys(data.paths[endpointPath]);

    operations.forEach((operation) => {
      const oprationName = endpointPath.replace('/api/', '').replace(/\//g, '');
      data.paths[endpointPath][operation].operationId = oprationName;
    });
  });

  return data;
}

instance
  .get('https://localhost:5001/swagger/v1/swagger.json')
  .then((response) => {
    const updatedSchema = addOperationIdsToSchema(response.data);
    fs.writeFileSync(
      path.resolve(__dirname, '../src/api/api-schema.json'),
      JSON.stringify(updatedSchema, null, 2),
    );

    console.log('==> Schema fetched successfully...');
  })
  .catch(console.error);