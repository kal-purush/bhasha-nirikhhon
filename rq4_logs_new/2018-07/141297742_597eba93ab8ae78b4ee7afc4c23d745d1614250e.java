package ru.sbt.serialization;

import java.io.Serializable;

/**
 * Created by Home on 17.07.2018.
 */
public class SerializablePoint extends SomePoint implements Serializable {
    public SerializablePoint( double x, double y, String name ) {
        super( x, y, name );
    }

    private Object writeReplace() {
        return new SerializationProxy(this);
    }

    private static class SerializationProxy implements Serializable {
        private String name;
        private double x;
        private double y;

        public SerializationProxy( SerializablePoint point ) {
            this.name = point.getName();
            this.x = point.getX();
            this.y = point.getY();
            //EnumSet
        }

        private Object readResolve() {
            return new SerializablePoint( x, y, name );
        }

    }
}