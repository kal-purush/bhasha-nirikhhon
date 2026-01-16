package post;

import lombok.Getter;

@Getter
public class NoSuchPostException extends RuntimeException{

    private Long postId;

    public NoSuchPostException() {
    }

    public NoSuchPostException(Long postId) {
        super(postId.toString());
        this.postId = postId;
    }

    public NoSuchPostException(String message) {
        super(message);
    }

    public NoSuchPostException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoSuchPostException(Throwable cause) {
        super(cause);
    }

    public NoSuchPostException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}