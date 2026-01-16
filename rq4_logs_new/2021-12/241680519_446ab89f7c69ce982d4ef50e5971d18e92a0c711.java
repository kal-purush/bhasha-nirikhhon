package br.ufsc.lapesd.freqel.server.results;

import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.util.indexed.FullIndexSet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Function;

import static br.ufsc.lapesd.freqel.server.results.ChunkedEncoderUtils.toFlux;
import static reactor.core.publisher.Mono.just;

public abstract class AbstractSVChunkedEncoder implements ChunkedEncoder {
    protected char sep;
    protected @Nonnull String eol;

    public AbstractSVChunkedEncoder(char sep, @Nonnull String eol) {
        this.sep = sep;
        this.eol = eol;
    }

    protected abstract void writeVarName(@Nonnull ByteBuf bb, @Nonnull String name,
                                         @Nonnull Charset charset);
    protected abstract void writeTerm(@Nonnull ByteBuf bb, @Nonnull Term term,
                                      @Nonnull Charset charset);

    @Override
    public @Nonnull Publisher<ByteBuf> encode(@Nonnull ByteBufAllocator allocator,
                                              @Nonnull Results results, boolean isAsk,
                                              @Nullable Charset cs) {
        if (cs == null)
            cs = StandardCharsets.UTF_8;
        if (isAsk)
            return just(Unpooled.wrappedBuffer((results.hasNext() ? eol+eol : eol).getBytes(cs)));
        List<String> vars = FullIndexSet.fromDistinct(results.getVarNames());
        Encoder encoder = new Encoder(allocator, vars, cs);
        return Flux.concat(just(encoder.createHeader()), toFlux(results).map(encoder));
    }

    private class Encoder implements Function<Solution, ByteBuf> {
        private final @Nonnull ByteBufAllocator allocator;
        private final int capacity;
        private final List<String> vars;
        private final @Nonnull Charset charset;
        private final  byte[] eolBytes;

        private Encoder(@Nonnull ByteBufAllocator allocator, @Nonnull List<String> vars,
                        @Nonnull Charset charset) {
            this.allocator = allocator;
            this.capacity = vars.size()*64;
            this.vars = vars;
            this.charset = charset;
            this.eolBytes = AbstractSVChunkedEncoder.this.eol.getBytes(charset);
        }

        public @Nonnull ByteBuf createHeader() {
            ByteBuf bb = allocator.buffer(capacity);
            boolean first = true;
            for (String var : vars) {
                if (first) first = false;
                else       bb.writeByte(sep);
                writeVarName(bb, var, charset);
            }
            return bb.writeBytes(eolBytes);
        }


        @Override public @Nonnull ByteBuf apply(@Nonnull Solution solution) {
            ByteBuf bb = allocator.buffer(capacity);
            for (String var : vars) {
                Term term = solution.get(var);
                if (term != null)
                    writeTerm(bb, term, charset);
                bb.writeByte(sep);
            }
            if (!vars.isEmpty())
                bb.writerIndex(bb.writerIndex()-1); //remove last sep
            return bb.writeBytes(eolBytes);
        }
    }

}