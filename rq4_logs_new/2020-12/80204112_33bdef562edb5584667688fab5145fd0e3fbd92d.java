package bayern.steinbrecher.green2.launcher.utility;

import javafx.beans.property.ReadOnlyDoubleProperty;
import javafx.beans.property.ReadOnlyDoubleWrapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public final class ProgressWrapper {
    private final ReadableByteChannel wrapped;
    private final double maxValue;
    private final ReadOnlyDoubleWrapper currentValue = new ReadOnlyDoubleWrapper(0);
    private final ReadOnlyDoubleWrapper progress = new ReadOnlyDoubleWrapper(0);

    public ProgressWrapper(ReadableByteChannel toWrap, double maxValue) {
        this.wrapped = new ReadableByteChannel() {
            @Override
            public int read(ByteBuffer dst) throws IOException {
                int numReadBytes = toWrap.read(dst);
                if (numReadBytes > 0) {
                    currentValue.set(currentValue.get() + numReadBytes);
                }
                return numReadBytes;
            }

            @Override
            public boolean isOpen() {
                return toWrap.isOpen();
            }

            @Override
            public void close() throws IOException {
                toWrap.close();
            }
        };
        this.maxValue = maxValue;
        progress.bind(currentValueProperty().divide(maxValue));
    }

    public ReadableByteChannel getWrapped() {
        return wrapped;
    }

    public double getMaxValue() {
        return maxValue;
    }

    public ReadOnlyDoubleProperty currentValueProperty() {
        return currentValue.getReadOnlyProperty();
    }

    public double getCurrentValue() {
        return currentValueProperty().get();
    }

    public ReadOnlyDoubleProperty progressProperty() {
        return progress.getReadOnlyProperty();
    }

    public double getProgress() {
        return progressProperty().get();
    }
}