package net.teengamingnights.assemblymod.config;

import com.google.common.io.ByteStreams;
import com.moandjiezana.toml.Toml;
import org.bukkit.plugin.Plugin;

import java.io.*;
import java.util.Optional;

public class TOMLWrapper<T> {

    private Plugin pl;
    private Class<T> object;

    private File file;
    private Toml toml;
    private T instance;

    public TOMLWrapper(Plugin pl, Class<T> object) {
        this.pl = pl;
        this.object = object;
    }

    public void load() throws IOException {
        String fileName = Optional.of(getClass().getAnnotation(FileName.class).fileName()).orElse("config.toml");
        file = new File(pl.getDataFolder(), fileName);

        if(!file.exists()) {
            file.createNewFile();
            InputStream in = pl.getResource(fileName);
            OutputStream out = new FileOutputStream(file);
            ByteStreams.copy(in, out);
            in.close();
            out.close();
        }

        toml = new Toml().read(file);
        instance = toml.to(object);
    }

    public T getInstance() {
        return instance;
    }
}