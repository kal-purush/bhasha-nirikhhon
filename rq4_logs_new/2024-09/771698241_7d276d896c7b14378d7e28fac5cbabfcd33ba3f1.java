package diruptio.spikedog.network;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import org.jetbrains.annotations.NotNull;

/** Initializes a channel with SSL encryption. */
public class EncryptedChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final SslContext sslContext;

    /**
     * Creates a new {@link EncryptedChannelInitializer}.
     *
     * @param sslContext The SSL context to use.
     */
    public EncryptedChannelInitializer(@NotNull SslContext sslContext) {
        this.sslContext = sslContext;
    }

    @Override
    protected void initChannel(SocketChannel channel) {
        channel.pipeline().addLast(sslContext.newHandler(channel.alloc()), new Http2OrHttpHandler());
    }
}