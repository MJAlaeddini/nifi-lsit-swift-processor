package org.nifi.processors;

import java.util.Collections;
import java.util.Set;

/**
 * Represents the listing state: the last timestamp processed,
 * the set of object names with that timestamp, and the set of entity-based keys.
 */
class ListingState {

    final long timestamp;
    final Set<String> timestampKeys;
    final Set<String> entityKeys;

    ListingState(long timestamp, Set<String> timestampKeys, Set<String> entityKeys) {
        this.timestamp = timestamp;
        this.timestampKeys = timestampKeys;
        this.entityKeys = entityKeys;
    }

    static ListingState empty() {
        return new ListingState(0L, Collections.emptySet(), Collections.emptySet());
    }
}
