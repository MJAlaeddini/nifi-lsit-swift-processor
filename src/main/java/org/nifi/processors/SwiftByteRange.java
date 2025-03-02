package org.nifi.processors;

import org.javaswift.joss.headers.object.range.AbstractRange;

public class SwiftByteRange extends AbstractRange {

    /**
     * Creates a range from 'start' to 'end' (inclusive).
     * In the parent, 'offset' = start, 'length' = end.
     */
    public SwiftByteRange(long start, long end) {
        super(start, end);
    }

    /**
     *  getFrom is the "start index" when copying bytes from the original array.
     */
    @Override
    public long getFrom(int totalSize) {
        // If offset is negative, interpret as 0
        if (offset < 0) {
            return 0;
        }
        // If offset > totalSize, it means beyond the length => treat as totalSize (no data)
        if (offset > totalSize) {
            return totalSize;
        }
        return offset;
    }

    /**
     *  getTo is the "end index" (exclusive) for copyOfRange(...)
     *  so if we want [start..end] inclusive, we return end+1
     */
    @Override
    public long getTo(int totalSize) {
        // If length < 0, we interpret that as "till the end"
        if (length < 0) {
            return totalSize;
        }
        // If length is beyond totalSize, clamp it
        if (length >= totalSize) {
            return totalSize;
        }
        // Since copyOfRange is exclusive for the 'to' index,
        // if we want to include 'length', we do length+1
        return length + 1;
    }
}
