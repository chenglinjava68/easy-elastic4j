package com.github.wens.elastic;

/**
 * Created by wens on 15-10-22.
 */
public class ElasticeException extends RuntimeException {

    public ElasticeException() {
    }

    public ElasticeException(String message) {
        super(message);
    }

    public ElasticeException(String message, Throwable cause) {
        super(message, cause);
    }

    public ElasticeException(Throwable cause) {
        super(cause);
    }

    public ElasticeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
