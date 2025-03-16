package org.nifi.processors;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.processor.ProcessSession;
import org.javaswift.joss.model.StoredObject;

import java.io.IOException;
import java.util.*;

public class TimestampObjectTracker implements ObjectTracker {

    private static final String STATE_TIMESTAMP = "swift.timestamp";
    private static final String STATE_KEY_PREFIX = "swift.key-";

    @Override
    public ListingState restoreState(ProcessSession session) throws IOException {
        StateMap stateMap = session.getState(Scope.CLUSTER);
        if (stateMap == null || stateMap.toMap().isEmpty()) {
            return ListingState.empty();
        }

        long restoredTimestamp = 0L;
        Set<String> restoredTimestampKeys = new HashSet<>();

        String tsValue = stateMap.get(STATE_TIMESTAMP);
        if (tsValue != null) {
            restoredTimestamp = Long.parseLong(tsValue);
        }

        Map<String, String> map = stateMap.toMap();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getKey().startsWith(STATE_KEY_PREFIX)) {
                restoredTimestampKeys.add(entry.getValue());
            }
        }

        return new ListingState(restoredTimestamp, restoredTimestampKeys, Collections.emptySet());
    }

    @Override
    public List<StoredObject> filterNewObjects(List<StoredObject> allObjects, ListingState currentState) {
        long lastTs = currentState.timestamp;
        Set<String> lastTsKeys = currentState.timestampKeys;

        List<StoredObject> newObjects = new ArrayList<>();
        for (StoredObject obj : allObjects) {
            long objLastModified = (obj.getLastModifiedAsDate() != null)
                    ? obj.getLastModifiedAsDate().getTime()
                    : 0L;

            // اگر timestamp آبجکت > lastTs
            // یا == lastTs ولی قبلا نامش در لیست Keys نبوده، پس جدید است
            if (objLastModified > lastTs
                    || (objLastModified == lastTs && !lastTsKeys.contains(obj.getName()))) {
                newObjects.add(obj);
            }
        }
        return newObjects;
    }

    @Override
    public void updateState(ProcessSession session, ListingState currentState, List<StoredObject> newObjects)
            throws IOException {

        long oldTimestamp = currentState.timestamp;
        Set<String> oldTimestampKeys = new HashSet<>(currentState.timestampKeys);

        long newTimestamp = oldTimestamp;
        Set<String> newTimestampKeys = new HashSet<>(oldTimestampKeys);

        for (StoredObject obj : newObjects) {
            long objLastModified = (obj.getLastModifiedAsDate() != null)
                    ? obj.getLastModifiedAsDate().getTime()
                    : 0L;
            if (objLastModified > newTimestamp) {
                newTimestamp = objLastModified;
                newTimestampKeys.clear();
                newTimestampKeys.add(obj.getName());
            } else if (objLastModified == newTimestamp) {
                newTimestampKeys.add(obj.getName());
            }
        }

        Map<String, String> newStateMap = new HashMap<>();
        newStateMap.put(STATE_TIMESTAMP, String.valueOf(newTimestamp));
        int index = 0;
        for (String name : newTimestampKeys) {
            newStateMap.put(STATE_KEY_PREFIX + index, name);
            index++;
        }

        session.setState(newStateMap, Scope.CLUSTER);
    }
}
