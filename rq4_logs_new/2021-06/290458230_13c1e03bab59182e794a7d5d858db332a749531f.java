package no.unit.nva.search;

import static nva.commons.core.attempt.Try.attempt;
import com.amazonaws.services.lambda.runtime.Context;
import java.net.HttpURLConnection;
import nva.commons.apigateway.ApiGatewayHandler;
import nva.commons.apigateway.RequestInfo;
import nva.commons.core.Environment;
import nva.commons.core.JacocoGenerated;

public class PrepareIndexForBatchInsertHandler extends ApiGatewayHandler<Void, Void> {

    private final ElasticSearchHighLevelRestClient es;

    @JacocoGenerated
    public PrepareIndexForBatchInsertHandler() {
        this(new Environment());
    }

    @JacocoGenerated
    public PrepareIndexForBatchInsertHandler(Environment environment) {
        this(environment, new ElasticSearchHighLevelRestClient());
    }

    public PrepareIndexForBatchInsertHandler(Environment environment, ElasticSearchHighLevelRestClient es) {
        super(Void.class, environment);
        this.es = es;
    }

    @Override
    protected Void processInput(Void input, RequestInfo requestInfo, Context context) {
        attempt(es::prepareIndexForBatchInsert).orElseThrow();
        return null;
    }

    @Override
    protected Integer getSuccessStatusCode(Void input, Void output) {
        return HttpURLConnection.HTTP_OK;
    }
}
