package com.github.darshan3105.exceptions;

/**
 * Thrown when there occurs any exception while merging the S3 files.
 */
public class FileMergingException extends RuntimeException {

    /**
     * Constructor.
     *
     * @param message Message
     */
    public FileMergingException(final String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param message Message
     * @param t Throwable
     */
    public FileMergingException(final String message, final Throwable t) {
        super(message, t);
    }
}
