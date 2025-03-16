package org.nifi.processors;

import org.apache.nifi.processor.ProcessSession;
import org.javaswift.joss.model.StoredObject;

import java.io.IOException;
import java.util.List;

public interface ObjectTracker {

    ListingState restoreState(ProcessSession session) throws IOException;

    List<StoredObject> filterNewObjects(List<StoredObject> allObjects, ListingState currentState);

    void updateState(ProcessSession session, ListingState currentState, List<StoredObject> newObjects) throws IOException;

}
