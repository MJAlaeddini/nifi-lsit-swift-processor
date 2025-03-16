package org.nifi.processors;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;

import java.io.IOException;
import java.util.*;

@PrimaryNodeOnly
@TriggerSerially
@TriggerWhenEmpty
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"OpenStack", "Swift", "object-storage", "list"})
@CapabilityDescription("Lists objects from a Swift container with optional name regex filtering and uses NiFi state to avoid duplicates.")
@Stateful(scopes = Scope.CLUSTER, description = "Stores info to avoid re-listing objects.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class ListSwift extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All listed objects are routed to success")
            .build();

    // Container name
    public static final PropertyDescriptor CONTAINER = new PropertyDescriptor.Builder()
            .name("swift-container")
            .displayName("Container")
            .description("Name of the Swift container to list.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    // Optional prefix
    public static final PropertyDescriptor PREFIX = new PropertyDescriptor.Builder()
            .name("swift-prefix")
            .displayName("Prefix")
            .description("Optional prefix filter for the objects to list.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    // Page size
    public static final PropertyDescriptor PAGE_SIZE = new PropertyDescriptor.Builder()
            .name("page-size")
            .displayName("Page Size")
            .description("Number of objects to retrieve per request.")
            .required(false)
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    // Optional regex filter
    public static final PropertyDescriptor OBJECT_NAME_REGEX = new PropertyDescriptor.Builder()
            .name("object-name-regex")
            .displayName("Object Name Regex")
            .description("Optional regular expression to filter object names. If set, only objects with names matching this regex will be listed.")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final AllowableValue TRACK_TIMESTAMP = new AllowableValue("timestamp", "Timestamp",
            "Track objects by lastModified timestamp");
    public static final AllowableValue TRACK_ENTITY = new AllowableValue("entity", "Entity",
            "Track objects by name for deduplication");

    public static final PropertyDescriptor TRACKING_STRATEGY = new PropertyDescriptor.Builder()
            .name("tracking-strategy")
            .displayName("Tracking Strategy")
            .description("Use 'timestamp' or 'entity' to avoid duplicates.")
            .required(true)
            .allowableValues(TRACK_TIMESTAMP, TRACK_ENTITY)
            .defaultValue(TRACK_TIMESTAMP.getValue())
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS;

    private SwiftContainerFactory swiftContainerFactory;
    private SwiftService swiftService;
    private ObjectTracker objectTracker;

    private volatile int pageSize;
    private volatile String fileNameRegex;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.addAll(SwiftContainerFactory.SWIFT_COMMON_PROPERTIES);
        props.add(CONTAINER);
        props.add(PREFIX);
        props.add(PAGE_SIZE);
        props.add(OBJECT_NAME_REGEX);
        props.add(TRACKING_STRATEGY);
        PROPERTY_DESCRIPTORS = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        this.swiftContainerFactory = new SwiftContainerFactory(context);
        this.swiftService = swiftContainerFactory.createSwiftService();

        this.pageSize = context.getProperty(PAGE_SIZE).asInteger();
        this.fileNameRegex = context.getProperty(OBJECT_NAME_REGEX).getValue();

        String tracking = context.getProperty(TRACKING_STRATEGY).getValue();
        if ("entity".equalsIgnoreCase(tracking)) {
            this.objectTracker = new EntityObjectTracker();
        } else {
            this.objectTracker = new TimestampObjectTracker();
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        ComponentLog logger = getLogger();

        ListingState currentState;
        try {
            currentState = objectTracker.restoreState(session);
        } catch (IOException e) {
            logger.error("Failed to restore state", e);
            context.yield();
            return;
        }

        String containerName = context.getProperty(CONTAINER).getValue();
        String prefix = context.getProperty(PREFIX).isSet() ? context.getProperty(PREFIX).getValue() : null;

        List<StoredObject> discovered = new ArrayList<>();
        String marker = null;

        while (true) {
            List<StoredObject> page;
            try {
                Container container = swiftService.getContainer(containerName);
                page = swiftService.listObjects(container, prefix, marker, pageSize);
            } catch (SwiftException ex) {
                logger.error("Swift error while listing objects: " + ex.getMessage(), ex);
                context.yield();
                return;
            }

            if (page.isEmpty()) {
                break;
            }

            for (StoredObject obj : page) {
                try {
                    obj.reload();
                } catch (Exception e) {
                    logger.warn("Failed to reload metadata for {}: {}", new Object[]{obj.getName(), e.getMessage()});
                    continue;
                }
            }

            List<StoredObject> afterRegex = new ArrayList<>();
            if (fileNameRegex != null && !fileNameRegex.isEmpty()) {
                for (StoredObject obj : page) {
                    if (obj.getName().matches(fileNameRegex)) {
                        afterRegex.add(obj);
                    }
                }
            } else {
                afterRegex = page;
            }

            List<StoredObject> newObjects = objectTracker.filterNewObjects(afterRegex, currentState);
            discovered.addAll(newObjects);

            StoredObject lastObj = page.get(page.size() - 1);
            marker = lastObj.getName();

            if (page.size() < pageSize) {
                break;
            }
        }

        if (discovered.isEmpty()) {
            context.yield();
            return;
        }

        for (StoredObject obj : discovered) {
            FlowFile flowFile = session.create();
            Map<String, String> attrs = SwiftAttributeBuilder.fromStoredObject(containerName, obj.getName(), obj);
            flowFile = session.putAllAttributes(flowFile, attrs);
            session.transfer(flowFile, REL_SUCCESS);
        }

        try {
            objectTracker.updateState(session, currentState, discovered);
        } catch (IOException e) {
            logger.error("Failed to update state: {}", e.getMessage(), e);
        }
    }
}
