package org.nifi.processors;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.processor.ProcessSession;
import org.javaswift.joss.model.StoredObject;

import java.io.IOException;
import java.util.*;

public class EntityObjectTracker implements ObjectTracker {

    private static final String STATE_ENTITY_KEY_PREFIX = "swift.entity-";

    @Override
    public ListingState restoreState(ProcessSession session) throws IOException {
        StateMap stateMap = session.getState(Scope.CLUSTER);
        if (stateMap == null || stateMap.toMap().isEmpty()) {
            return ListingState.empty();
        }
        Set<String> entityKeys = new HashSet<>();
        Map<String, String> map = stateMap.toMap();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getKey().startsWith(STATE_ENTITY_KEY_PREFIX)) {
                entityKeys.add(entry.getValue());
            }
        }
        // اینجا timestamp کاربردی ندارد
        return new ListingState(0L, Collections.emptySet(), entityKeys);
    }

    @Override
    public List<StoredObject> filterNewObjects(List<StoredObject> allObjects, ListingState currentState) {
        Set<String> knownEntities = currentState.entityKeys;
        List<StoredObject> newObjects = new ArrayList<>();
        for (StoredObject obj : allObjects) {
            if (!knownEntities.contains(obj.getName())) {
                newObjects.add(obj);
            }
        }
        return newObjects;
    }

    @Override
    public void updateState(ProcessSession session, ListingState currentState, List<StoredObject> newObjects)
            throws IOException {

        Set<String> updatedEntities = new HashSet<>(currentState.entityKeys);
        for (StoredObject obj : newObjects) {
            updatedEntities.add(obj.getName());
        }

        Map<String, String> newStateMap = new HashMap<>();
        int idx = 0;
        for (String entity : updatedEntities) {
            newStateMap.put(STATE_ENTITY_KEY_PREFIX + idx, entity);
            idx++;
        }
        session.setState(newStateMap, Scope.CLUSTER);
    }
}
