package org.nifi.processors;

import java.io.Serial;

/**
 * Custom exception for handling fetch-specific errors.
 */
class SwiftFetchException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    public SwiftFetchException(String message) {
        super(message);
    }

    public SwiftFetchException(String message, Throwable cause) {
        super(message, cause);
    }
}
