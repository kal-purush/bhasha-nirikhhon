package com.github.updatedvoxelmap.plugin;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import net.md_5.bungee.api.chat.BaseComponent;
import net.md_5.bungee.chat.ComponentSerializer;

/**
 * A data output stream that contains some additional methods to write data in minecraft format.
 */
public class MinecraftDataOutputStream extends DataOutputStream {
    public MinecraftDataOutputStream(OutputStream out) {
        super(out);
    }

    public void writeString(String string) throws IOException {
        byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
        writeVarInt(bytes.length);
        out.write(bytes);
    }

    public void writeVarInt(int value) throws IOException {
        while ((value & 0xFFFFFF80) != 0) {
            writeByte(value & 0x7F | 0x80);
            value >>>= 7;
        }
        writeByte(value);
    }

    public void writeVarLong(long value) throws IOException {
        while ((value & 0xFFFFFFFFFFFFFF80L) != 0) {
            writeByte((int) (value & 0x7F | 0x80));
            value >>>= 7;
        }
        writeByte((int) value);
    }

    public void writeUuid(UUID uuid) throws IOException {
        writeLong(uuid.getMostSignificantBits());
        writeLong(uuid.getLeastSignificantBits());
    }

    public void writeText(BaseComponent... text) throws IOException {
        writeString(ComponentSerializer.toString(text));
    }

    public void writeText(BaseComponent text) throws IOException {
        writeString(ComponentSerializer.toString(text));
    }
}