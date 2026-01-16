package aws.lambda.dynamodb;

import aws.lambda.dynamodb.bean.EventRequest;
import aws.lambda.dynamodb.bean.EventResponse;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class SaveEventHandler implements RequestHandler<EventRequest, EventResponse> {

  private DynamoDB dynamoDb;

  private String DYNAMODB_TABLE_NAME = "Events";
  private Regions REGION = Regions.EU_WEST_1;

  public EventResponse handleRequest(EventRequest personRequest, Context context) {
    this.initDynamoDbClient();

    persistData(personRequest);

    EventResponse personResponse = new EventResponse();
    personResponse.setMessage("Saved Successfully!!!");
    return personResponse;
  }

  private PutItemOutcome persistData(EventRequest personRequest) throws ConditionalCheckFailedException {
    return this.dynamoDb.getTable(DYNAMODB_TABLE_NAME)
        .putItem(
            new PutItemSpec().withItem(new Item()
                .withNumber("id", personRequest.getId())
                .withString("Source", personRequest.getSource())
                .withString("Date", personRequest.getDate())
                .withString("Severity", personRequest.getSeverity())
                .withString("Message", personRequest.getMessage())));
  }

  private void initDynamoDbClient() {
    AmazonDynamoDBClient client = new AmazonDynamoDBClient();
    client.setRegion(Region.getRegion(REGION));
    this.dynamoDb = new DynamoDB(client);
  }
}