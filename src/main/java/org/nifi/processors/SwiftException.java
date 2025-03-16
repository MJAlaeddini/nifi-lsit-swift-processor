package org.nifi.processors;

import java.io.Serial;
import org.apache.nifi.processor.exception.ProcessException;

/**
 * A custom exception for handling Swift-related errors in a uniform way.
 */
public class SwiftException extends ProcessException {

    @Serial
    private static final long serialVersionUID = 1L;

    public SwiftException(String message) {
        super(message);
    }

    public SwiftException(String message, Throwable cause) {
        super(message, cause);
    }
}
