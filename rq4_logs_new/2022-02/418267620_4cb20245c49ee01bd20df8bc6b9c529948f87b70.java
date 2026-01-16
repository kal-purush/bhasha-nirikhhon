package dev.stocky37.xiv.actions.xivapi;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.RateLimiter;
import dev.stocky37.xiv.actions.data.Job;
import dev.stocky37.xiv.actions.data.JobConverter;
import dev.stocky37.xiv.actions.data.Query;
import dev.stocky37.xiv.actions.util.Util;
import dev.stocky37.xiv.actions.xivapi.json.SearchBody;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.eclipse.microprofile.rest.client.inject.RestClient;

@SuppressWarnings("UnstableApiUsage")
@ApplicationScoped
public class XivApiClient {
	private final XivApi xivapi;
	private final RateLimiter rateLimiter;
	private final Function<JsonNode, Job> jobConverter;
	private final Util util;

	@Inject
	public XivApiClient(
		@RestClient XivApi xivapi,
		RateLimiter rateLimiter,
		JobConverter jobConverter,
		Util util
	) {
		this.xivapi = xivapi;
		this.rateLimiter = rateLimiter;
		this.jobConverter = jobConverter;
		this.util = util;
	}

	public <T> List<T> search(Query query, Function<JsonNode, T> converter) {
		final SearchBody body = new SearchBody(
			String.join(",", query.indexes()),
			String.join(",", query.columns()),
			util.toJsonNode(query)
		);

		return wrapApi(() -> xivapi.search(body).Results().parallelStream().map(converter).toList());
	}

	public List<Job> getJobs() {
		return wrapApi(() -> xivapi.getClassJobs(JobConverter.ALL_FIELDS).Results()
			.parallelStream()
			.map(jobConverter)
			.collect(Collectors.toList()));
	}

	private <T> T wrapApi(Supplier<T> supplier) {
		rateLimiter.acquire();
		try {
			return supplier.get();
		} catch (WebApplicationException e) {
			if(e.getResponse().getStatusInfo() == Response.Status.NOT_FOUND) {
				throw new NotFoundException(e);
			} else {
				throw new InternalServerErrorException(e);
			}
		}
	}
}