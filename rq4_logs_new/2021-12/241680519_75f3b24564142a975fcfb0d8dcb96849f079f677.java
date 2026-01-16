package br.ufsc.lapesd.freqel.query.endpoint.impl;

import br.ufsc.lapesd.freqel.algebra.Cardinality;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.cardinality.EstimatePolicy;
import br.ufsc.lapesd.freqel.description.AskDescription;
import br.ufsc.lapesd.freqel.description.Description;
import br.ufsc.lapesd.freqel.model.NTParseException;
import br.ufsc.lapesd.freqel.model.SPARQLString;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.std.StdTermFactory;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.endpoint.*;
import br.ufsc.lapesd.freqel.query.endpoint.exceptions.DQEndpointException;
import br.ufsc.lapesd.freqel.query.endpoint.exceptions.QueryExecutionException;
import br.ufsc.lapesd.freqel.query.modifiers.Ask;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.ArraySolution;
import br.ufsc.lapesd.freqel.query.results.impl.PublisherResults;
import br.ufsc.lapesd.freqel.util.indexed.FullIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import com.google.common.base.Stopwatch;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetAddress;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static br.ufsc.lapesd.freqel.algebra.Cardinality.*;
import static br.ufsc.lapesd.freqel.model.RDFUtils.fromNT;
import static java.util.Collections.synchronizedSet;

public class CompliantTSVSPARQLClient extends AbstractTPEndpoint implements DQEndpoint {
    private static final Logger logger = LoggerFactory.getLogger(CompliantTSVSPARQLClient.class);
    private static final byte[] URI_NEEDS_ESCAPE;
    private static final char[] HEX = "0123456789ABCDEF".toCharArray();
    private static final String TSV = "text/tab-separated-values";
    private static final String TSV_SHORT = "text/tsv";
    private static final Pattern CHARSET_RX = Pattern.compile("charset\\s*=\\s*([^ \n\r\t;]+)");

    public static final String DEF_ACCEPT = "text/tab-separated-values; charset=utf-8, text/tab-separated-values; q=0.9";

    static {
        // As per RFC 2396 the following scharacters are subject to percent-escaping (except in
        // locations where they are expected in the URI grammar):
        //
        // reserved    = ";" | "/" | "?" | ":" | "@" | "&" | "=" | "+" |
        //               "$" | ","
        // control     = <US-ASCII coded characters 00-1F and 7F hexadecimal>
        // space       = <US-ASCII coded character 20 hexadecimal>
        // delims      = "<" | ">" | "#" | "%" | <">
        // unwise      = "{" | "}" | "|" | "\" | "^" | "[" | "]" | "`"
        byte[] need = ";/?:@&=+$,\u007F <>#%\"{}|\\^[]`".getBytes(StandardCharsets.UTF_8);
        Arrays.sort(need);
        URI_NEEDS_ESCAPE = need;
    }

    private final @Nonnull String uri;
    private final char paramChar;
    private final @Nonnull Map<String, String> headerValues = new HashMap<>();
    private final @Nonnull Map<String, String> queryParams = new HashMap<>();
    private final @Nonnull Set<PublisherResults.Canceller> active = synchronizedSet(new HashSet<>());
    private boolean closed = false;
    private CompletableFuture<String> resolvedURI;

    public CompliantTSVSPARQLClient(@Nonnull String uri) {
        this(uri, AskDescription::new);
    }

    public CompliantTSVSPARQLClient(@Nonnull String uri,
                                    @Nonnull Function<TPEndpoint, ? extends Description> factory) {
        super(factory);
        this.uri = uri;
        this.paramChar = uri.indexOf('?') > uri.indexOf('@') ? '&' : '?';
        headerValues.put("accept", DEF_ACCEPT);
        if (uri.startsWith("http://")) {
            resolvedURI = Mono.fromCallable(() -> {
                Stopwatch sw = Stopwatch.createStarted();
                String host = new URL(uri).getHost();
                String address = InetAddress.getByName(host).getHostAddress();
                String resolved = uri.replaceFirst(host, address);
                logger.debug("Resolved {} to {} in {}", uri, resolved, sw);
                return resolved;
            }).subscribeOn(Schedulers.boundedElastic()).retry(4).onErrorReturn(uri).toFuture();
        }
    }

    /* --- --- --- Configuration --- --- --- */

    /**
     * Set a value for a HTTP request header to be sent on subsequent requests.
     *
     * @param header the header name. Internally all header names canonized to lower case.
     *               The Accept header cannot be set via this method, since its value will
     *               depend on whether the query is an ASK or SELECT query.
     * @param value The value for the HTTP request header. This value will be sent as-is and
     *              no validation is performed. If value is null, the given header will not
     *              be set in future requests.
     * @throws IllegalArgumentException if header is {@code accept}.
     * @return the old value set for the given header, which may be null.
     */
    public @Nullable String setHeader(@Nonnull String header, @Nullable String value) {
        header = header.trim().toLowerCase();
        if (value == null) {
            if (header.equals("accept"))
                headerValues.put(header, DEF_ACCEPT);
            else
                return headerValues.remove(header);
        }
        if (header.equals("accept"))
            throw new IllegalArgumentException("Cannot set the accept header, use set(Ask)Accept.");
        return headerValues.put(header, value);
    }

    /**
     * Set a fixed query parameter to be sent along each query
     *
     * @param name the parameter name, cannot be empty nor {@code query}.
     * @param value the parameter value. If null will remove any value set for the parameter
     * @param valueEncoded whether the value is already percent-escaped.
     * @return The old value assigned to the parameter.
     */
    public @Nullable String setQueryParam(@Nonnull String name, @Nullable String value,
                                          boolean valueEncoded) {
        if (value == null)
            return queryParams.remove(name);
        if (name.equals("query"))
            throw new IllegalArgumentException("query URI param cannot be fixed");
        return queryParams.put(name, valueEncoded ? value : escape(value));
    }

    public @Nonnull String prettyPrint() {
        StringBuilder b = new StringBuilder()
                .append("CompliantTSVSPARQLClient[").append(uri).append(']');
        if (headerValues.size() > 1 || !headerValues.get("accept").equals(DEF_ACCEPT)) {
            b.append("{\n  headers={\n");
            for (Map.Entry<String, String> e : headerValues.entrySet()) {
                b.append("    ").append(e.getKey()).append(": ").append(e.getValue()).append(",\n");
            }
            b.setLength(b.length()-2);
            b.append("\n  }");
        }
        if (!queryParams.isEmpty()) {
            b.append(b.charAt(b.length()-1) == ']'
                    ? "{\n  queryParams={\n"
                    : ", \n  queryParams={\n");
            for (Map.Entry<String, String> e : queryParams.entrySet())
                b.append("    ").append(e.getKey()).append("=").append(e.getValue()).append(",\n");
            b.setLength(b.length()-2);
            b.append("\n  }");
        }
        if (b.charAt(b.length()-1) != ']')
            b.append("\n}");
        return b.toString();
    }


    /* --- --- --- Implement AbstractTPEndpoint --- --- --- */

    @Override public @Nonnull DisjunctiveProfile getDisjunctiveProfile() {
        return SPARQLDisjunctiveProfile.DEFAULT;
    }

    @Override public @Nonnull Results query(@Nonnull Op query)
            throws DQEndpointException, QueryExecutionException {
        if (closed)
            throw new IllegalStateException(this+" is close()d!");
        SPARQLString ss = SPARQLString.create(query);
        Results results = querySPARQL(ss.getSparql(), ss.isAsk(), ss.getVarNames());
        results.setOptional(query.modifiers().optional() != null);
        return results;
    }

    @Override public @Nonnull Results query(@Nonnull CQuery query) {
        if (closed)
            throw new IllegalStateException(this+" is close()d!");
        SPARQLString ss = SPARQLString.create(query);
        Results results = querySPARQL(ss.getSparql(), ss.isAsk(), ss.getVarNames());
        results.setOptional(query.getModifiers().optional() != null);
        return results;
    }

    @Override public @Nonnull Cardinality estimate(@Nonnull CQuery query, int policy) {
        if (query.isEmpty()) return EMPTY;

        if (EstimatePolicy.canQueryRemote(policy) || EstimatePolicy.canAskRemote(policy)) {
            if (closed)
                throw new IllegalStateException(this+" is close()d!");
            MutableCQuery askQuery = new MutableCQuery(query);
            if (askQuery.getModifiers().ask() == null)
                askQuery.mutateModifiers().add(Ask.INSTANCE);
            try (Results results = query(askQuery)) {
                return results.hasNext() ? NON_EMPTY : EMPTY;
            } catch (QueryExecutionException e) { return EMPTY; }
        }
        return UNSUPPORTED;
    }

    @Override public boolean hasSPARQLCapabilities() {
        return true;
    }

    @Override public boolean hasRemoteCapability(@Nonnull Capability capability) {
        switch (capability) {
            case PROJECTION:
            case DISTINCT:
            case CARTESIAN:
            case LIMIT:
            case SPARQL_FILTER:
            case VALUES:
            case OPTIONAL:
            case ASK:
                return true;
            default:
                return false;
        }
    }

    @Override public void close() {
        if (closed)
            return;
        closed = true;
        ArrayList<PublisherResults.Canceller> copy = new ArrayList<>(active);
        for (PublisherResults.Canceller c : copy)
            c.cancel();
    }

    /* --- --- --- Implement DQEndpoint --- --- --- */

    @Override public boolean canQuerySPARQL() {
        return true;
    }

    @Override public @Nonnull Results querySPARQL(@Nonnull String sparqlQuery) {
        if (closed)
            throw new IllegalStateException(this+" is close()d!");
        Query parsed = QueryFactory.create(sparqlQuery);
        return querySPARQL(sparqlQuery, parsed.isAskType(), parsed.getResultVars());
    }

    @Override
    public @Nonnull Results querySPARQL(@Nonnull String sparqlQuery, boolean isAsk,
                                        @Nonnull Collection<String> varNamesCollection) {
        if (closed)
            throw new IllegalStateException(this+" is close()d!");
        IndexSet<String> vars = FullIndexSet.fromDistinct(varNamesCollection);
        Flux<Solution> solutions = HttpClient.create()
                .headers(this::setupHeaders)
                .get().uri(buildURI(sparqlQuery))
                .response((resp, bbFlux) -> {
                    try {
                        String ct = getContentType(resp);
                        if (!ct.startsWith(TSV) && !ct.startsWith(TSV_SHORT))
                            throw new QueryExecutionException(this, "Bad Content-Type=\""+ct+"\"");
                        return bbFlux.concatMap(new TSVParser(vars, getCharset(ct)));
                    } catch (Throwable t) {
                        return Mono.error(QueryExecutionException.wrap(t, this));
                    }
                });
        PublisherResults results = new PublisherResults(solutions, vars);
        PublisherResults.Canceller canceller = results.createCanceller(active::remove);
        canceller.ifActive(active::add);
        assert !closed : "Concurrent query and close() call";
        return results;
    }

    /* --- --- --- Implement Object --- --- --- */

    @Override public @Nonnull String toString() {
        return "CompliantTSVSPARQLClient["+uri+"]";
    }
    /* --- --- --- Private methods --- --- --- */

    private @Nonnull String getResolvedURI() {
        try {
            return resolvedURI.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) throw (RuntimeException) e.getCause();
            if (e.getCause() instanceof Error)            throw (Error) e.getCause();
            throw new RuntimeException(e.getCause() == null ? e : e.getCause());
        } catch (InterruptedException e) {
            return uri;
        }
    }

    private @Nonnull String buildURI(@Nonnull String sparqlQuery) {
        String uri = getResolvedURI();
        StringBuilder uriBuilder = new StringBuilder(uri.length()+ sparqlQuery.length()*2);
        uriBuilder.append(uri).append(paramChar).append("query=").append(escape(sparqlQuery));
        for (Map.Entry<String, String> e : queryParams.entrySet())
            uriBuilder.append('&').append(e.getKey()).append('=').append(e.getValue());
        return uriBuilder.toString();
    }

    private void setupHeaders(@Nonnull HttpHeaders b) {
        for (String k : headerValues.keySet())
            b.set(k, headerValues.get(k));
    }

    static String escape(@Nonnull String unescaped) {
        StringBuilder b = new StringBuilder(unescaped.length()*2);
        for (int i = 0, len = unescaped.length(); i < len; i++) {
            char c = unescaped.charAt(i);
            if (c <= 0x1F || (c < 128 && Arrays.binarySearch(URI_NEEDS_ESCAPE, (byte)c) >= 0)) {
                b.append('%').append(HEX[(c>>4)]).append(HEX[c&0x0F]);
            } else {
                b.append(c);
            }
        }
        return b.toString();
    }

    private @Nonnull String getContentType(@Nonnull HttpClientResponse response) {
        String ct = response.responseHeaders().get(HttpHeaderNames.CONTENT_TYPE);
        return ct == null || ct.isEmpty() ? "text/tab-separated-values" : ct;
    }

    private @Nonnull Charset getCharset(@Nonnull String contentType) {
        Matcher m = CHARSET_RX.matcher(contentType);
        if (m.find())
            return Charset.forName(m.group(1));
        return StandardCharsets.UTF_8;
    }

    public static class BadTSVException extends RuntimeException {
        public BadTSVException(@Nonnull String message) {
            super(message);
        }
    }

    class TSVParser implements Function<ByteBuf, Publisher<Solution>> {
        private final IndexSet<String> vars;
        private final Charset charset;
        private final ArraySolution.ValueFactory factory;
        private boolean header;
        private @Nullable int[] projection;
        private @Nullable String carry;

        public TSVParser(IndexSet<String> vars, Charset charset) {
            this.vars = vars;
            this.charset = charset;
            factory = ArraySolution.forVars(vars);
        }

        @Override public Publisher<Solution> apply(ByteBuf bb) {
            try {
                String str = bb.toString(charset);
                if (carry != null) {
                    str = carry + str;
                    carry = null;
                }
                int firstEnd = str.indexOf('\n');
                if (firstEnd == str.length()-1) {
                    return parseSingleRow(str);
                } else {
                    return parseMultiRow(str, firstEnd);
                }
            } catch (Throwable t) {
                return Mono.error(new QueryExecutionException(CompliantTSVSPARQLClient.this, t));
            }
        }

        private @Nonnull Flux<Solution> parseMultiRow(String str, int firstEnd) {
            List<Solution> list = new ArrayList<>();
            int begin = 0, end = firstEnd;
            while (end >= begin) {
                if (!header)
                    parseHeader(str, begin, end);
                else {
                    list.add(parseSolution(str, begin, end));
                }
                end = str.indexOf('\n', begin = end + 1);
            }
            if (begin < str.length())
                carry = str.substring(begin);
            return Flux.fromIterable(list);
        }

        private @Nonnull Mono<Solution> parseSingleRow(String str) {
            if (!header) {
                parseHeader(str, 0, str.length()-1);
                return Mono.empty();
            } else {
                return Mono.just(parseSolution(str, 0, str.length()-1));
            }
        }

        private @Nonnull Solution parseSolution(@Nonnull String str, int begin, int end) {
            assert header;
            assert end == str.length() || str.charAt(end) == '\n';
            Term[] terms = new Term[vars.size()];
            for (int i = 0, pos = begin, nVars = vars.size(); i < nVars; i++) {
                int termEnd = str.indexOf('\t', pos);
                termEnd = termEnd == -1 ? end : termEnd;
                try {
                    terms[i] = fromNT(str.substring(pos, termEnd), StdTermFactory.INSTANCE);
                } catch (NTParseException e) {
                    logger.error("{}: Discarding invalid NT term: {}",
                                 CompliantTSVSPARQLClient.this, e);
                }
                pos = termEnd+1;
            }
            if (projection != null) {
                Term[] projected = new Term[projection.length];
                for (int i = 0; i < projected.length; i++)
                    projected[i] = projection[i] >= 0 ? terms[projection[i]] : null;
                terms = projected;
            }
            return factory.fromValues(terms);
        }

        private void parseHeader(@Nonnull String str, int begin, int end) {
            assert !header;
            assert str.charAt(end) == '\n';
            header = true;
            if (begin == end)
                return; // ASK response
            if (str.charAt(begin) != '?')
                throw new BadTSVException("Variables must start with ?, got " + str.charAt(begin) + "");
            if (str.charAt(end) != '\n')
                throw new BadTSVException("TSV header row does not end in \\n");
            List<String> actualVars = new ArrayList<>();
            int i = begin+1, sepIdx = str.indexOf('\t', begin);
            while (sepIdx >= 0 && sepIdx < end) {
                actualVars.add(str.substring(i, sepIdx));
                if (str.charAt(sepIdx+1) != '?')
                    throw new BadTSVException("Variables must start with '?'");
                sepIdx = str.indexOf('\t', i = sepIdx + 2);
            }
            if (i < end)
                actualVars.add(str.substring(i, end));
            if (!actualVars.equals(vars)) {
                logger.warn("Remote endpoint {} changed projection from {} to {}! " +
                            "Will compensate", uri, vars, actualVars);
                projection = new int[vars.size()];
                for (int j = 0, size = vars.size(); j < size; j++)
                    projection[j] = actualVars.indexOf(vars.get(j));
            }
        }
    }
}