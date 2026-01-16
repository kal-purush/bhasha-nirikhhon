package io.camunda.connector;

import io.camunda.connector.api.annotation.OutboundConnector;
import io.camunda.connector.api.outbound.OutboundConnectorContext;
import io.camunda.connector.api.outbound.OutboundConnectorFunction;
import io.camunda.connector.retrofit.SeatService;
import io.camunda.connector.retrofit.login.LoginRequest;
import io.camunda.connector.retrofit.login.LoginResponse;
import io.camunda.connector.retrofit.seat.CreateSeatRequest;
import okhttp3.OkHttpClient;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

import java.io.IOException;

@OutboundConnector(
        name = "Create seat",
        inputVariables = {"email", "password", "apiUrl", "forDate", "businessUuid"},
        type = "apendo:create-seat-single:1")
public class SeatSingleFunction implements OutboundConnectorFunction {

    @Override
    public Object execute(OutboundConnectorContext context) throws Exception {
        var connectorRequest = context.getVariablesAsType(SeatSingleRequest.class);

        context.validate(connectorRequest);
        context.replaceSecrets(connectorRequest);

        return executeConnector(connectorRequest);
    }

    private SeatSingleResult executeConnector(final SeatSingleRequest request) {

        OkHttpClient.Builder httpClient = new OkHttpClient.Builder();
        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(request.getApiUrl())
                .addConverterFactory(GsonConverterFactory.create())
                .client(httpClient.build())
                .build();

        SeatService service = retrofit.create(SeatService.class);

        var loginRequest = LoginRequest.builder()
                .email(request.getEmail())
                .password(request.getPassword())
                .build();

        var call = service.LOGIN_CALL(loginRequest);
        LoginResponse loginResponse;
        try {
            loginResponse = call.execute().body();
        } catch (IOException e) {
            throw new RuntimeException(e.getLocalizedMessage());
        }

        var splitDate = request.getForDate().split("-");
        var forYearMonth = splitDate[0] + "/" + splitDate[1];

        var seatRequest = CreateSeatRequest.builder()
                .businessUuid(request.getBusinessUuid())
                .forYearMonth(forYearMonth)
                .build();
        var seatCall = service.CREATE_SEAT_RESPONSE_CALL("Bearer " + loginResponse.getAccessToken(), seatRequest);

        try {
            var response = seatCall.execute();
            if (response.isSuccessful()) {
                var seatSingleResult = new SeatSingleResult();
                seatSingleResult.setSeatResponse(response.body());
                return seatSingleResult;

            } else
                throw new IllegalArgumentException(response.code() + ": Failed to create seat: " + response.message());

        } catch (IOException e) {
            throw new RuntimeException(e.getLocalizedMessage());
        }
    }
}