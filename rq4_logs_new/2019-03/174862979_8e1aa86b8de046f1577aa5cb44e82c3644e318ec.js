export function handler(event, context, callback) {
  const query = event.queryStringParameters;
  console.log("hello called:", event.queryStringParameters);

  callback(null, {
    statusCode: 200,
    body: JSON.stringify({ msg: "hello world:" + query.a })
  });
}