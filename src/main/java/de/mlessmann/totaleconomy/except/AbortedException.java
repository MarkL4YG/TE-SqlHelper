package de.mlessmann.totaleconomy.except;

/**
 * Created by MarkL4YG on 15-May-18
 */
public class AbortedException extends RuntimeException {

    public AbortedException() {
    }

    public AbortedException(String message) {
        super(message);
    }

    public AbortedException(String message, Throwable cause) {
        super(message, cause);
    }

    public AbortedException(Throwable cause) {
        super(cause);
    }

    public AbortedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
