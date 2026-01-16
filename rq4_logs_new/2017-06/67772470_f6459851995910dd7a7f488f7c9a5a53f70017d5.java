package nablarch.common.databind;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.Map;

import nablarch.common.databind.csv.CsvDataBindConfig;

/**
 * {@link ObjectMapperFactory}の基本実装クラス。
 *
 * @author siosio
 */
public class BasicObjectMapperFactory extends ObjectMapperFactory {

    /**
     * {@link ObjectMapper}を生成する。
     *
     * @param clazz データとのバインディングを行うクラス
     * @param stream 入力ストリーム
     * @param <T> バインディング対象のJavaのクラス
     * @return データとJava ObjectのMapper
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> ObjectMapper<T> createMapper(final Class<T> clazz, final InputStream stream) {
        final DataBindConfig dataBindConfig = DataBindUtil.createDataBindConfig(clazz);
        final MapperType type = toMapperType(clazz, dataBindConfig);

        if (type == MapperType.CSV_BEAN) {
            return type.createMapper(clazz, dataBindConfig, stream);
        }

        // 到達しない
        throw new IllegalArgumentException("Unsupported config or class. class = [" + toFQCN(clazz) + "],"
                + " config = [" + toFQCN(dataBindConfig) + ']');
    }

    /**
     * {@link ObjectMapper}を生成する。
     *
     * @param clazz データとのバインディングを行うクラス
     * @param stream 入力ストリーム
     * @param dataBindConfig マッピング設定
     * @param <T> バインディング対象のJavaのクラス
     * @return データとJava ObjectのMapper
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> ObjectMapper<T> createMapper(final Class<T> clazz, final InputStream stream, final DataBindConfig dataBindConfig) {
        final MapperType type = toMapperType(clazz, dataBindConfig);

        if (type == MapperType.CSV_BEAN) {
            throw new IllegalArgumentException("this class should not be set config. class = [" + toFQCN(clazz) + ']');
        } else if (type == MapperType.CSV_MAP) {
            return type.createMapper(clazz, dataBindConfig, stream);
        }
        // 到達しない
        throw new IllegalArgumentException("Unsupported config or class. class = [" + toFQCN(clazz) + "],"
                + " config = [" + toFQCN(dataBindConfig) + ']');
    }

    /**
     * {@link ObjectMapper}を生成する。
     *
     * @param clazz データとのバインディングを行うクラス
     * @param reader 入力ストリーム
     * @param <T> バインディング対象のJavaのクラス
     * @return データとJava ObjectのMapper
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> ObjectMapper<T> createMapper(final Class<T> clazz, final Reader reader) {
        final DataBindConfig dataBindConfig = DataBindUtil.createDataBindConfig(clazz);
        final MapperType type = toMapperType(clazz, dataBindConfig);

        if (type == MapperType.CSV_BEAN) {
            return type.createMapper(clazz, dataBindConfig, reader);
        }
        // 到達しない
        throw new IllegalArgumentException("Unsupported config or class. class = [" + toFQCN(clazz) + "],"
                + " config = [" + toFQCN(dataBindConfig) + ']');
    }

    /**
     * {@link ObjectMapper}を生成する。
     *
     * @param clazz データとのバインディングを行うクラス
     * @param reader 入力ストリーム
     * @param dataBindConfig マッピング設定
     * @param <T> バインディング対象のJavaのクラス
     * @return データとJava ObjectのMapper
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> ObjectMapper<T> createMapper(final Class<T> clazz, final Reader reader, final DataBindConfig dataBindConfig) {
        final MapperType type = toMapperType(clazz, dataBindConfig);

        if (type == MapperType.CSV_BEAN) {
            throw new IllegalArgumentException("this class should not be set config. class = [" + toFQCN(clazz) + ']');
        } else if (type == MapperType.CSV_MAP) {
            return type.createMapper(clazz, dataBindConfig, reader);
        }
        // 到達しない
        throw new IllegalArgumentException("Unsupported config or class. class = [" + toFQCN(clazz) + "],"
                + " config = [" + toFQCN(dataBindConfig) + ']');
    }

    /**
     * {@link ObjectMapper}を生成する。
     *
     * @param clazz データとのバインディングを行うクラス
     * @param stream 出力ストリーム
     * @param <T> バインディング対象のJavaのクラス
     * @return データとJava ObjectのMapper
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> ObjectMapper<T> createMapper(final Class<T> clazz, final OutputStream stream) {
        final DataBindConfig dataBindConfig = DataBindUtil.createDataBindConfig(clazz);
        final MapperType type = toMapperType(clazz, dataBindConfig);

        if (type == MapperType.CSV_BEAN) {
            return type.createMapper(clazz, dataBindConfig, stream);
        }
        // 到達しない
        throw new IllegalArgumentException("Unsupported config or class. class = [" + toFQCN(clazz) + "],"
                + " config = [" + toFQCN(dataBindConfig) + ']');
    }

    /**
     * {@link ObjectMapper}を生成する。
     *
     * @param clazz データとのバインディングを行うクラス
     * @param stream 出力ストリーム
     * @param dataBindConfig マッピング設定
     * @param <T> バインディング対象のJavaのクラス
     * @return データとJava ObjectのMapper
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> ObjectMapper<T> createMapper(final Class<T> clazz, final OutputStream stream, final DataBindConfig dataBindConfig) {
        final MapperType type = toMapperType(clazz, dataBindConfig);

        if (type == MapperType.CSV_BEAN) {
            throw new IllegalArgumentException("this class should not be set config. class = [" + toFQCN(clazz) + ']');
        } else if (type == MapperType.CSV_MAP) {
            return type.createMapper(clazz, dataBindConfig, stream);
        }
        // 到達しない
        throw new IllegalArgumentException("Unsupported config or class. class = [" + toFQCN(clazz) + "],"
                + " config = [" + toFQCN(dataBindConfig) + ']');
    }

    /**
     * {@link ObjectMapper}を生成する。
     *
     * @param clazz データとのバインディングを行うクラス
     * @param writer Writer
     * @param <T> バインディング対象のJavaのクラス
     * @return データとJava ObjectのMapper
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> ObjectMapper<T> createMapper(final Class<T> clazz, final Writer writer) {
        final DataBindConfig dataBindConfig = DataBindUtil.createDataBindConfig(clazz);
        final MapperType type = toMapperType(clazz, dataBindConfig);

        if (type == MapperType.CSV_BEAN) {
            return type.createMapper(clazz, dataBindConfig, writer);
        }
        // 到達しない
        throw new IllegalArgumentException("Unsupported config or class. class = [" + toFQCN(clazz) + "],"
                + " config = [" + toFQCN(dataBindConfig) + ']');
    }

    /**
     * {@link ObjectMapper}を生成する。
     *
     * @param clazz データとのバインディングを行うクラス
     * @param writer Writer
     * @param dataBindConfig マッピング設定
     * @param <T> バインディング対象のJavaのクラス
     * @return データとJava ObjectのMapper
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> ObjectMapper<T> createMapper(final Class<T> clazz, final Writer writer, final DataBindConfig dataBindConfig) {
        final MapperType type = toMapperType(clazz, dataBindConfig);

        if (type == MapperType.CSV_BEAN) {
            throw new IllegalArgumentException("this class should not be set config. class = [" + toFQCN(clazz) + ']');
        } else if (type == MapperType.CSV_MAP) {
            return type.createMapper(clazz, dataBindConfig, writer);
        }
        // 到達しない
        throw new IllegalArgumentException("Unsupported config or class. class = [" + toFQCN(clazz) + "],"
                + " config = [" + toFQCN(dataBindConfig) + ']');
    }

    /**
     * マッパーのタイプを返す。
     *
     * @param clazz クラス
     * @param dataBindConfig 設定情報
     * @return マッパーのタイプ
     */
    protected MapperType toMapperType(final Class<?> clazz, final DataBindConfig dataBindConfig) {
        if (Map.class.isAssignableFrom(clazz) && dataBindConfig instanceof CsvDataBindConfig) {
            return MapperType.CSV_MAP;
        } else if (dataBindConfig instanceof CsvDataBindConfig) {
            return MapperType.CSV_BEAN;
        }
        throw new IllegalArgumentException("Unsupported config or class. class = [" + toFQCN(clazz) + "],"
                + " config = [" + toFQCN(dataBindConfig) + ']');
    }

    /**
     * クラスからFQCNを返す。
     * <p/>
     * nullの場合は、文字列のnullを
     *
     * @param object オブジェクト
     * @return FQCN
     */
    private static String toFQCN(final Object object) {
        if (object == null) {
            return "null";
        }
        if (object instanceof Class) {
            return ((Class<?>) object).getName();
        } else {
            return object.getClass()
                         .getName();
        }
    }
}