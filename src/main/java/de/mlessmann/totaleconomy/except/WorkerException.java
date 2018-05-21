package de.mlessmann.totaleconomy.except;

/**
 * Created by MarkL4YG on 14-May-18
 */
public class WorkerException extends RuntimeException {

    public WorkerException() {
    }

    public WorkerException(String message) {
        super(message);
    }

    public WorkerException(String message, Throwable cause) {
        super(message, cause);
    }

    public WorkerException(Throwable cause) {
        super(cause);
    }

    public WorkerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
