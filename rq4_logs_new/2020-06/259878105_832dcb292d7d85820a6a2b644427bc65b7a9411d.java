package org.folio.rest.client;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import org.folio.exception.EntityNotFoundException;
import org.folio.rest.jaxrs.model.OpeningPeriod;
import org.folio.rest.jaxrs.model.OpeningPeriods;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;

public class CalendarClient extends OkapiClient {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String PERIODS_QUERY_PARAMS = "servicePointId=%s&startDate=%s&endDate=%s&includeClosedDays=%s";
  private static final String OPENING_PERIODS = "openingPeriods";
  private static final String DATE_KEY = "date";
  private static final String ALL_DAY_KEY = "allDay";
  private static final String OPEN_KEY = "open";
  private static final String OPENING_HOUR_KEY = "openingHour";
  private static final String OPENING_DAY_KEY = "openingDay";
  private static final String DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
  private static final String DATE_PART_FORMAT = "yyyy-MM-dd";
  private static final DateTimeFormatter DATE_TIME_FORMATTER =
    DateTimeFormat.forPattern(DATE_TIME_FORMAT).withZoneUTC();

  public CalendarClient(Vertx vertx, Map<String, String> okapiHeaders) {
    super(WebClient.create(vertx), okapiHeaders);
  }

  public Future<Collection<OpeningPeriod>> fetchOpeningDaysBetweenDates(String servicePointId,
    DateTime startDate, DateTime endDate, boolean includeClosedDays) {

    Promise<HttpResponse<Buffer>> promise = Promise.promise();

    String params = String.format(PERIODS_QUERY_PARAMS,
      servicePointId, startDate.toLocalDate(), endDate.toLocalDate().plusDays(1), includeClosedDays);
    String path = String.format("/calendar/periods?%s", params);

    getAbs(path).send(promise);

    return promise.future().compose(response -> {
      int responseStatus = response.statusCode();
      if (responseStatus != 200) {
        String errorMessage = String.format("Failed to fetch by ID: %s. Response: %d %s",
          path, responseStatus, response.bodyAsString());
        log.error(errorMessage);
        return failedFuture(new EntityNotFoundException(errorMessage));
      } else {
        try {
          OpeningPeriods openingPeriods = objectMapper.readValue(
            response.bodyAsString(), OpeningPeriods.class);

          return succeededFuture(Optional.ofNullable(openingPeriods)
            .map(OpeningPeriods::getOpeningPeriods)
            .orElse(new ArrayList<>()));

        } catch (JsonProcessingException e) {
          log.error("Failed to parse response: " + response.bodyAsString());
          return failedFuture(e);
        }
      }
    });
  }

//  public static OpeningDay fromOpeningPeriodJson(JsonObject openingPeriod, DateTimeZone zone) {
//    JsonObject openingDay = openingPeriod.getJsonObject(OPENING_DAY_KEY);
//    String dateProperty = openingPeriod.getString(DATE_KEY);
//    DateTime date = null;
//    if (dateProperty != null) {
//      date = DateTime.parse(dateProperty, DATE_TIME_FORMATTER).withZoneRetainFields(zone);
//    }
//
//    return new OpeningDay(openingDay, date);
//  }
}