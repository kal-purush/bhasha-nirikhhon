package pl.com.softproject.utils.freshmail.hash;

import org.apache.commons.codec.digest.DigestUtils;

import java.io.Serializable;

/**
 * Class HtmlHashGenerator
 *
 * @author Marcin Jasiński {@literal <mkjasinski@gmail.com>}
 */
public abstract class AbstractHashGenerator implements Serializable, HashGenerator {

    @Override
    public String generate(String apiKey, String getData, String postData, String apiSecret) {
        return DigestUtils.sha1Hex(apiKey + getData + postData + apiSecret);
    }
}