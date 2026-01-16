package dk.trustworks.intranet.utils;

import io.quarkus.panache.common.Sort;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public final class SortBuilder {

    private static final Set<String> ALLOWED = Set.of(
            "invoicenumber","uuid","clientname","projectname",
            "sumnotax","type","invoicedate","bookingdate"
    );

    public static Sort from(List<String> tokens) {

        // 1 – fall-back order when no valid token is present
        if (tokens == null || tokens.isEmpty()
                || (tokens.size() == 1 && tokens.getFirst().isBlank())) {
            return Sort.descending("invoicedate");          // newest first
        }

        Sort result = null;                                 // ← accumulator

        for (String token : tokens) {
            String[] parts = token.split(",");
            String field = parts[0].trim().toLowerCase(Locale.ROOT);
            String dir   = parts.length > 1 ? parts[1].trim() : "asc";

            if (!ALLOWED.contains(field)) {
                throw new WebApplicationException(
                        Response.status(400)
                                .entity(Map.of("error",
                                        "Unsupported sort field: " + field))
                                .build());
            }

            // 2 – create/extend the Sort using *column* not *Sort* argument
            Sort.Direction direction = dir.equalsIgnoreCase("desc")
                    ? Sort.Direction.Descending
                    : Sort.Direction.Ascending;

            if (result == null) {
                result = Sort.by(field, direction);         // first column
            } else {
                result = result.and(field, direction);      // extra columns
            }
        }
        return result;
    }


    private SortBuilder() {}                          // util class – no ctor
}