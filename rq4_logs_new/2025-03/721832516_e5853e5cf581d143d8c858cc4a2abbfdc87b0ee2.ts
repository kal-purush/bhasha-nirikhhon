import { APIGatewayProxyEvent, APIGatewayProxyResult } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";
import jwt, { JwtPayload } from "jsonwebtoken";

export const handler = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  return getCharacters(event);
};

interface Parameters {
  userId: string;
  characterShort: boolean;
}

async function getCharacters(event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> {
  try {
    const params = verifyRequest(event);

    console.log(`Get characters for user ${params.userId} from DynamoDB`);

    // https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/javascriptv3/example_code/dynamodb/actions/document-client/get.js
    const client = new DynamoDBClient({});
    const docClient = DynamoDBDocumentClient.from(client);
    const command = new QueryCommand({
      TableName: process.env.TABLE_NAME,
      KeyConditionExpression: "userId = :userId",
      ExpressionAttributeValues: {
        ":userId": params.userId,
      },
      ConsistentRead: true,
    });

    const dynamoDbResponse = await docClient.send(command);

    if (!dynamoDbResponse.Items || dynamoDbResponse.Items.length === 0) {
      console.error("No characters found for the given userId");
      throw {
        statusCode: 404,
        body: JSON.stringify({
          message: "No characters found for the given userId",
        }),
      };
    }

    console.log("Successfully got DynamoDB items");

    let characters = [];
    if (params.characterShort) {
      for (const item of dynamoDbResponse.Items) {
        characters.push({
          userId: item.userId,
          characterId: item.characterId,
          name: item.characterSheet.generalInformation.name,
          level: item.characterSheet.generalInformation.level,
        });
      }
    } else {
      characters = dynamoDbResponse.Items;
    }

    const response = {
      statusCode: 200,
      body: JSON.stringify({
        message: "Successfully got characters",
        characters: characters,
      }),
    };
    console.log(response);
    return response;
  } catch (error: any) {
    const response = {
      statusCode: error.statusCode ?? 500,
      body:
        error.body ??
        JSON.stringify({
          message: "An error occurred!",
          error: (error as Error).message,
        }),
    };
    console.error(response);

    return response;
  }
}

function verifyRequest(event: APIGatewayProxyEvent): Parameters {
  console.log("Verify request");

  // TODO move handling of authorization token to Lambda layer
  // Trim the authorization header as it could contain spaces at the beginning
  const authHeader = event.headers.Authorization?.trim() || event.headers.authorization?.trim();
  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    throw {
      statusCode: 401,
      body: JSON.stringify({ message: "Unauthorized: No token provided!" }),
    };
  }

  const token = authHeader.split(" ")[1]; // Remove "Bearer " prefix
  // Decode the token without verification (the access to the API itself is already protected by the authorizer)
  const decoded = jwt.decode(token) as JwtPayload | null;
  if (!decoded) {
    throw {
      statusCode: 401,
      body: JSON.stringify({ message: "Unauthorized: Invalid token!" }),
    };
  }

  const userId = decoded.sub; // Cognito User ID
  if (!userId) {
    throw {
      statusCode: 401,
      body: JSON.stringify({ message: "Unauthorized: User ID not found in token!" }),
    };
  }

  if (
    event.queryStringParameters?.["character-short"] &&
    typeof event.queryStringParameters?.["character-short"] !== "string"
  ) {
    console.error("Invalid input values!");
    throw {
      statusCode: 400,
      body: JSON.stringify({
        message: "Invalid input values!",
      }),
    };
  }

  const params: Parameters = {
    userId: userId,
    characterShort: event.queryStringParameters?.["character-short"] === "true",
  };

  return params;
}