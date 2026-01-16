package classloading.customreloader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.jar.JarFile;

public class JarReloader {
    private String jarName;
    private ClassLoader parentClassLoader;
    private static final Logger LOGGER = LogManager.getLogger(Main.class);

    public JarReloader(final String jarName, final ClassLoader parentClassLoader) {
        this.jarName = jarName;
        if (parentClassLoader == null) {
            this.parentClassLoader = this.getClass().getClassLoader();
        }
    }

    public JarReloader(String jarName) {
        this.jarName = jarName;
        this.parentClassLoader = this.getClass().getClassLoader();
    }

    public void reloadJar() throws IOException {
        File file = new File(jarName);
        JarFile jarFile = new JarFile(file);
        LOGGER.info("Scanning jar file - " + jarFile.getName() + " for classes to reload.");
        URL jarURL = file.toURI().toURL();
        jarFile.stream()
                .forEach(jarEntry -> {
                    String jarEntryName = jarEntry.getName();
                    if (jarEntry.isDirectory() || !jarEntryName.endsWith(".class")) {
                        LOGGER.info(jarEntryName + " - is not a class... ignoring.");
                    } else {
                        String className = jarEntryName.replaceAll("/", ".").replace(".class", "");
                        LOGGER.info(className + " - is loaded.");
                        try (MyUrlClassLoader urlClassLoader = new MyUrlClassLoader(new URL[]{jarURL}, parentClassLoader)) {
                            try {
                                urlClassLoader.loadClass(className);
                            } catch (ClassNotFoundException e) {
                                LOGGER.error(className + " was not found.", e);
                            }
                        } catch (IOException e) {
                            LOGGER.error(e);
                        }
                    }
                });
    }

    private class MyUrlClassLoader extends URLClassLoader {
        public MyUrlClassLoader(final URL[] urls, final ClassLoader parent) {
            super(urls, parent);
        }

        @Override
        public Class<?> loadClass(final String name) throws ClassNotFoundException {
            SecurityManager sm = System.getSecurityManager();
            if (sm != null) {
                int i = name.lastIndexOf('.');
                if (i != -1) {
                    sm.checkPackageAccess(name.substring(0, i));
                }
            }

            return super.loadClass(name);
        }

        private byte[] loadClassData(String className) throws IOException {
            try {
                /*
                 * get the actual path using the original classloader
                 */
                Class<?> clazz = getParent().loadClass(className);
                URL url = clazz.getResource(clazz.getSimpleName() + ".class");

                /*
                 * force reload
                 */
                File f = new File(url.toURI());
                int size = (int) f.length();
                byte buff[] = new byte[size];
                FileInputStream fis = new FileInputStream(f);
                DataInputStream dis = new DataInputStream(fis);
                dis.readFully(buff);
                dis.close();
                return buff;
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        }
    }

}