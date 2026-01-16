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
import br.ufsc.lapesd.freqel.query.results.impl.QueueResults;
import br.ufsc.lapesd.freqel.util.indexed.FullIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.*;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static br.ufsc.lapesd.freqel.algebra.Cardinality.*;
import static br.ufsc.lapesd.freqel.model.RDFUtils.fromNT;
import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings("UnusedReturnValue")
public class NettyCompliantTSVSPARQLClient extends AbstractTPEndpoint implements DQEndpoint {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(NettyCompliantTSVSPARQLClient.class);
    private static final byte[] URI_NEEDS_ESCAPE;
    private static final char[] HEX = "0123456789ABCDEF".toCharArray();
    private static final @Nonnull String HANDLER_NAME = "tsvParser";
    private static final String SPARQL_QUERY = "application/sparql-query";
    private static final String TSV = "text/tab-separated-values";
    private static final String TSV_SHORT = "text/tsv";
    private static final Pattern CHARSET_RX = Pattern.compile("charset\\s*=\\s*([^ \n\r\t;]+)");

    public static final String DEF_ACCEPT = "text/tab-separated-values; charset=utf-8, text/tab-separated-values; q=0.9";
    public static final int DEF_QUEUE_CAPACITY = 2048;

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
        byte[] need = ";/?:@&=+$,\u007F <>#%\"{}|\\^[]`".getBytes(UTF_8);
        Arrays.sort(need);
        URI_NEEDS_ESCAPE = need;
    }

    private static final @Nonnull NettyHttpClient nettyHttpClient
            = new NettyHttpClient(TSVParser.SUPPLIER, HANDLER_NAME);

    private final @Nonnull String uri;
    private final char paramChar;
    private int queueCapacity = DEF_QUEUE_CAPACITY;
    private HttpMethod method = HttpMethod.GET;
    private final @Nonnull Map<String, String> headerValues = new HashMap<>();
    private final @Nonnull Map<String, String> queryParams = new HashMap<>();
    private final @Nonnull NettyHttpClient.Targeted targeted;

    private boolean closed = false;

    public NettyCompliantTSVSPARQLClient(@Nonnull String uri) {
        this(uri, AskDescription::new);
    }

    public NettyCompliantTSVSPARQLClient(@Nonnull String uri,
                                         @Nonnull Function<TPEndpoint, ? extends Description> factory) {
        super(factory);
        this.uri = uri;
        this.paramChar = uri.indexOf('?') > uri.indexOf('@') ? '&' : '?';
        headerValues.put("accept", DEF_ACCEPT);
        targeted = nettyHttpClient.acquire(uri);
    }

    /* --- --- --- Configuration --- --- ---  */

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
     * Use HTTP POST requests sending the query as application/sparql-query in the request body.
     *
     * @return this {@link NettyCompliantTSVSPARQLClient}.
     */
    public @Nonnull NettyCompliantTSVSPARQLClient usePOST() {
        method = HttpMethod.POST;
        return this;
    }

    /**
     * Use HTTP GET requests with the query percent encoded into the {@code query} query parameter.
     *
     * This is the default method.
     *
     * @return this {@link NettyCompliantTSVSPARQLClient}.
     */
    public @Nonnull NettyCompliantTSVSPARQLClient useGET() {
        method = HttpMethod.GET;
        return this;
    }

    /**
     * Desired maximum number of queued solutions not yet consumed by {@link Results}
     * instances produced by this {@link NettyCompliantTSVSPARQLClient}.
     *
     * Once the queue consumed by a {@link Results} reaches this capacity, reading and parsing
     * of solutions stops, propagating backpressure to the OS and eventually to the application
     * layer of the remote SPARQL endpoint.
     *
     * Low values will cause frequent start/stop of reading and higher overhead. The default
     * is {@link NettyCompliantTSVSPARQLClient#DEF_QUEUE_CAPACITY}
     *
     * @param capacity the new queue capacity, should be {@code > 0}. If {@code <= 0}, will
     *                 fall back to {@link NettyCompliantTSVSPARQLClient#DEF_QUEUE_CAPACITY} with
     *                 a warning.
     * @return the old queue capacity
     */
    public int setQueueCapacity(int capacity) {
        if (capacity <= 0) {
            logger.warn("{}.setQueueCapacity({}): invalid capacity, falling back to {}",
                        this, capacity, DEF_QUEUE_CAPACITY);
            capacity = DEF_QUEUE_CAPACITY;
        }
        int old = this.queueCapacity;
        this.queueCapacity = capacity;
        return old;
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

    /* --- --- --- Implement AbstractTPEndpoint --- --- --- */

    @Nonnull @Override public DisjunctiveProfile getDisjunctiveProfile() {
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
        targeted.close();
    }

    /* --- --- --- Implement DQEndpoint --- --- --- */

    @Override public boolean canQuerySPARQL() {
        return true;
    }

    @Override public @Nonnull Results querySPARQL(@Nonnull String sparqlQuery) {
        if (closed)
            throw new IllegalStateException(this+" is close()d");
        Query parsed = QueryFactory.create(sparqlQuery);
        return querySPARQL(sparqlQuery, parsed.isAskType(), parsed.getResultVars());
    }

    @Override
    public @Nonnull Results querySPARQL(@Nonnull String sparqlQuery, boolean isAsk,
                                        @Nonnull Collection<String> varNames) {
        if (closed)
            throw new IllegalStateException(this+" is close()d");
        IndexSet<String> vars = FullIndexSet.fromDistinct(varNames);
        BlockingQueue<Solution> queue = new ArrayBlockingQueue<>(queueCapacity+2);
        QueueResults results = new QueueResults(varNames, queue);
        targeted.request(method, buildPath(sparqlQuery),
                method == HttpMethod.GET ? null : a -> {
                    ByteBuf bb = a.buffer(sparqlQuery.length() + 32);
                    bb.writeCharSequence(sparqlQuery, UTF_8);
                    return bb;
                },
                (ch, request) -> {
                    TSVParser p = (TSVParser) ch.pipeline().get(HANDLER_NAME);
                    results.onClose(p.setup(vars, queue, queueCapacity, QueueResults.DEFAULT_END,
                                            targeted.getPool(), ch, uri));
                    results.afterConsume(p.getOnConsume());
                    ch.config().setAutoRead(true);
                    for (Map.Entry<String, String> e : headerValues.entrySet())
                        request.headers().set(e.getKey(), e.getValue());
                    if (method == HttpMethod.POST)
                        request.headers().set(HttpHeaderNames.CONTENT_TYPE, SPARQL_QUERY);
                }
        ).addListener(f -> {
            if (!f.isSuccess()) {
                logger.info("Couldn't connect to {}, no results for {}", uri, sparqlQuery);
                queue.add(QueueResults.DEFAULT_END);
            }
        });
        return results;
    }

    /* --- --- --- Implement Object --- --- --- */

    @Override public @Nonnull String toString() {
        return "NettyCompliantTSVSPARQLClient{"+uri+"}";
    }

    /* --- --- --- Private methods --- --- --- */

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

    private @Nonnull String buildPath(@Nonnull String sparql) {
        boolean get = method == HttpMethod.GET;
        String bp = targeted.basePath();
        int capacity = bp.length() + queryParams.size() * 32 + sparql.length() * (get ? 2 : 0);
        StringBuilder b = new StringBuilder(capacity).append(bp).append(paramChar);
        if (get)
            b.append("query=").append(escape(sparql)).append('&');
        for (Map.Entry<String, String> e : queryParams.entrySet())
            b.append(e.getKey()).append('=').append(e.getValue()).append('&');
        b.setLength(b.length()-1);
        return b.toString();
    }

    public static class BadTSVException extends IllegalArgumentException {
        public BadTSVException(String messge) {
            super(messge);
        }
    }

    private static class TSVParser extends SimpleChannelInboundHandler<HttpObject> {
        private static final Logger logger = LoggerFactory.getLogger(TSVParser.class);

        private static final @Nonnull Supplier<TSVParser> SUPPLIER = new Supplier<TSVParser>() {
            @Override public @Nonnull TSVParser get() { return new TSVParser(); }
            @Override public @Nonnull String toString() { return "TSVParser::new"; }
        };

        private int[] projection = null;
        private @Nullable ArraySolution.ValueFactory factory = null;
        private @Nullable Queue<Solution> queue;
        private @Nullable SocketChannel channel;
        private @Nullable Solution endMarker;
        private @Nullable ChannelPool pool;
        private boolean badResponse;
        private @Nullable String baseURI;
        private @Nonnull Charset charset = UTF_8;
        private @Nullable String carry;
        private int queueCapacity = DEF_QUEUE_CAPACITY;
        private @Nullable IndexSet<String> expectedVars;
        private final @Nonnull List<String> tmpVars = new ArrayList<>();
        private final @Nonnull List<Term> tmpTerms = new ArrayList<>();
        private @Nullable List<Term> tmpProj;
        private @Nullable Closer closer;
        private final @Nonnull Runnable onConsume = new Runnable() {
            private final @Nonnull Runnable unsafe = () -> {
                if (queue != null && channel != null && queue.size() <= queueCapacity/2)
                    channel.config().setAutoRead(true);
            };

            @Override public void run() {
                if (channel != null)
                    channel.eventLoop().execute(unsafe);
            }
        };

        private class Closer implements Runnable {
            private final @Nonnull Channel myChannel;
            boolean enableAbortingClose;

            private Closer(@Nonnull Channel myChannel) {
                this.myChannel = myChannel;
            }

            @Override public void run() {
                // enableAbortingClose must be checked from within the event loop (to avoid
                // racing against release()). The first check is just to avoid a needless schedule
                if (enableAbortingClose) {
                    myChannel.eventLoop().execute(() -> {
                        assert myChannel.eventLoop().inEventLoop();
                        if (enableAbortingClose) {
                            assert channel == myChannel;
                            myChannel.close();
                        }
                    });
                }
            }
        }

        @Override public boolean isSharable() { return false; }

        public @Nonnull Runnable getOnConsume() {
            return onConsume;
        }

        public @Nonnull Runnable setup(@Nonnull IndexSet<String> vars,
                                       @Nonnull Queue<Solution> queue,
                                       int queueCapacity, @Nonnull Solution endMarker,
                                       @Nullable ChannelPool pool, @Nonnull SocketChannel channel,
                                       @Nonnull String baseURI) {
            this.baseURI = baseURI;
            this.charset = UTF_8;
            this.badResponse = false;
            this.expectedVars = vars;
            this.queue = queue;
            this.queueCapacity = queueCapacity;
            this.endMarker = endMarker;
            this.pool = pool;
            return this.closer = new Closer(this.channel = channel);
        }

        private void enqueue(@Nullable Solution solution) {
            try {
                if (queue != null) {
                    if (solution == null)
                        logger.error("Unexpected null solution on {}", this);
                    else
                        queue.add(solution);
                } else
                    logger.error("Unexpected null queue on {}", this);
            } catch (Throwable t) {
                logger.error("Unexpected {} on queue.add({}) on {}",
                             t.getClass().getSimpleName(), solution, this, t);
            }
        }

        private void release(@Nonnull Channel channel) {
            if (this.channel == null)
                return;
            logger.trace("{}.release({})", this, channel);
            assert channel.eventLoop().inEventLoop();
            assert this.channel == channel : "TSVParser sharing or missed setup()/release()";
            enqueue(endMarker);
            queue = null;
            // do not close a channel being returned to the pool
            if (closer != null)
                closer.enableAbortingClose = false;
            closer = null;
            factory = null;
            this.channel = null;
            if (pool != null)
                pool.release(channel);
            pool = null;
            badResponse = false;
        }

        @Override public String toString() {
            StringBuilder b = new StringBuilder("TSVParser");
            if (channel != null && !channel.remoteAddress().isUnresolved())
                b.append('[').append(baseURI).append(']');
            b.append("{ch=").append(channel);
            if (queueCapacity != DEF_QUEUE_CAPACITY)
                b.append(", queueCapacity=").append(queueCapacity);
            if (expectedVars != null)
                b.append(", expectedVars=").append(expectedVars);
            return b.append('}').toString();
        }

        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.warn("Exception when handling channel {}", ctx.channel(), cause);
            ctx.close();
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) {
            logger.trace("{}.channelInactive()", this);
            release(ctx.channel());
        }

        @Override protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
            if (queue == null)
                throw new IllegalStateException("setup() not called!");
            assert ctx.channel() == channel : "setup() not called or TSVParser is being shared";
            if (msg instanceof HttpResponse) {
                handleResponse(ctx, (HttpResponse)msg);
            }
            if (msg instanceof HttpContent) {
                if (badResponse) {
                    String string = ((HttpContent) msg).content().toString(UTF_8);
                    logger.info("Body chunk of bad response on {}: {}", this, string);
                } else {
                    handleChunk(ctx, (HttpContent) msg);
                    if (msg instanceof LastHttpContent)
                        release(ctx.channel());
                }
            }
        }

        private void handleChunk(@Nonnull ChannelHandlerContext ctx,
                                 @Nonnull HttpContent httpContent) {
            assert queue != null;
            if (httpContent.content().readableBytes() == 0)
                return;
            String str = httpContent.content().toString(charset);
            if (carry != null) {
                str = carry + str;
                carry = null;
            }
            int b = 0, e = str.indexOf('\n');
            if (e < 0) {
                carry = str;
                return;
            }
            if (factory == null) {
                parseHeader(str, e);
                e = str.indexOf('\n', b = e+1);
            }
            while (e >= 0) {
                enqueue(parseRow(str, b, e));
                e = str.indexOf('\n', b = e+1);
            }
            if (b < str.length())
                carry = str.substring(b);
            if (queue.size() >= queueCapacity) //backpressure
                ctx.channel().config().setAutoRead(false);
        }

        private void parseHeader(@Nonnull String str, int end) {
            if (end == 0) {
                factory = ArraySolution.EMPTY_FACTORY;
                return;
            }
            if (str.charAt(end) != '\n')
                throw new IllegalArgumentException("Header not ending in \\n: "+str);
            tmpVars.clear();
            for (int varBegin = 0, sep; varBegin < end; varBegin = sep + 1) {
                if (str.charAt(varBegin) != '?')
                    throw new BadTSVException("Variables must start with ?, got"+str);
                sep = Math.min(str.indexOf('\t', varBegin), end);
                tmpVars.add(str.substring(varBegin+1, sep < 0 ? sep = end : sep));
            }
            assert expectedVars != null;
            if (!tmpVars.equals(expectedVars)) {
                projection = new int[expectedVars.size()];
                for (int i = 0; i < projection.length; i++)
                    projection[i] = tmpVars.indexOf(expectedVars.get(i));
                tmpProj = new ArrayList<>();
            }
            factory = ArraySolution.forVars(expectedVars);
        }

        private @Nonnull Solution parseRow(@Nonnull String str, int begin, int end) {
            assert factory != null;
            assert queue != null;
            tmpTerms.clear();
            if (begin == end)
                return factory.fromValues(Collections.emptyList());
            int pos = begin;
            while (pos <= end) {
                int sep = Math.min(str.indexOf('\t', pos), end);
                String nt = str.substring(pos, sep < 0 ? sep = end : sep);
                try {
                    tmpTerms.add(nt.isEmpty() ? null : fromNT(nt, StdTermFactory.INSTANCE));
                } catch (NTParseException e) {
                    logger.warn("Discarding term {} on {}. Reason: {}", nt, this, e.getMessage());
                    tmpTerms.add(null);
                }
                pos = sep+1;
            }
            if (projection != null) {
                assert tmpProj != null;
                tmpProj.clear();
                for (int i : projection) tmpProj.add(tmpTerms.get(i));
                return factory.fromValues(tmpProj);
            }
            return factory.fromValues(tmpTerms);
        }

        private void handleResponse(@Nonnull ChannelHandlerContext ignoredCtx,
                                    @Nonnull HttpResponse r) {
            assert queue != null;
            if (r.status().codeClass() != HttpStatusClass.SUCCESS) {
                badResponse = true;
                logger.warn("Bad response status {} on {}", r.status(), this);
            } else {
                String type = r.headers().get(HttpHeaderNames.CONTENT_TYPE, TSV);
                if (!type.startsWith(TSV) && !type.startsWith(TSV_SHORT)) {
                    logger.warn("Invalid Content-Type {} on {}, expected {}", type, this, TSV);
                    badResponse = true;
                } else {
                    Matcher m = CHARSET_RX.matcher(type);
                    if (m.find()) {
                        try {
                            charset = Charset.forName(m.group(1));
                        } catch (IllegalCharsetNameException e) {
                            logger.warn("Bad charset {} on {}, using UTF-8", m.group(1), this);
                        }
                    }
                }
            }
        }
    }

}