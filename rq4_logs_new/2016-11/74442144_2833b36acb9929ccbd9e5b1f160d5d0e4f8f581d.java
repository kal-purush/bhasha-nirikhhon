package protocol;

/**
 * Created by matt on 24/11/16.
 * <p>
 * This class contains {@link MESSAGE} which is used to
 * define the actions that JavashipsClient can perform.
 */
public final class JavashipsProtocol {
    public static enum MESSAGE {
        GAMEOVER,
        READY,
        ATTACK
    }
}