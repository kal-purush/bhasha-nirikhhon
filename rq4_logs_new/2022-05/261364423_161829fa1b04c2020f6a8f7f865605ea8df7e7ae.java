package elixter.blog.dto.post;

import elixter.blog.domain.hashtag.Hashtag;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public abstract class AbstractPostDto {

    protected Long id;

    protected List<String> hashtags = new ArrayList<>();


    protected List<Hashtag> getHashtagAsInstance() {
        List<Hashtag> result = new ArrayList<>();

        if (hashtags != null) {
            for (String tag : hashtags) {
                Hashtag hashtag = new Hashtag();
                hashtag.setTag(tag);
                hashtag.setPostId(id);
                result.add(hashtag);
            }
        }

        return result;
    }
}