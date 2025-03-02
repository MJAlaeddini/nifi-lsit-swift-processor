/**
 * NiFi Processor that lists objects from Swift.
 * <p>
 * This processor connects to a Swift container, applies age and size filters on stored objects,
 * and creates FlowFiles with object attributes. It supports both timestamp-based and entity-based
 * tracking to avoid duplicate processing, with state stored in NiFi's cluster state.
 * </p>
 */
package org.nifi.processors;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.scheduling.SchedulingStrategy;

import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@PrimaryNodeOnly
@TriggerSerially
@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"openstack", "swift", "object-storage", "list"})
@SeeAlso()
@CapabilityDescription("Lists objects from Swift, supports Entity or Timestamp tracking to avoid duplicates, plus size limits.")
@Stateful(scopes = Scope.CLUSTER, description = "Stores info to avoid re-listing.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
@WritesAttributes({
        @WritesAttribute(attribute = "swift.container", description = "Container name"),
        @WritesAttribute(attribute = "filename", description = "Object name"),
        @WritesAttribute(attribute = "swift.etag", description = "ETag"),
        @WritesAttribute(attribute = "swift.length", description = "Object size")
})
public class ListSwift extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All listed objects")
            .build();
    private static final Set<Relationship> RELATIONSHIPS = Collections.singleton(REL_SUCCESS);

    public static final PropertyDescriptor SWIFT_AUTH_URL = new PropertyDescriptor.Builder()
            .name("swift-auth-url")
            .displayName("Auth URL")
            .description("Swift auth endpoint")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final AllowableValue AUTH_TEMPAUTH = new AllowableValue("TEMPAUTH", "TempAuth", "v1 style");
    public static final AllowableValue AUTH_KEYSTONE = new AllowableValue("KEYSTONE", "Keystone", "Keystone style");
    public static final PropertyDescriptor AUTH_METHOD = new PropertyDescriptor.Builder()
            .name("swift-auth-method")
            .displayName("Auth Method")
            .description("TempAuth or Keystone")
            .required(true)
            .allowableValues(AUTH_TEMPAUTH, AUTH_KEYSTONE)
            .defaultValue(AUTH_TEMPAUTH.getValue())
            .build();

    public static final PropertyDescriptor SWIFT_USERNAME = new PropertyDescriptor.Builder()
            .name("swift-username")
            .displayName("Username")
            .description("Swift user, e.g. 'test:tester' for TempAuth")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SWIFT_PASSWORD = new PropertyDescriptor.Builder()
            .name("swift-password")
            .displayName("Password")
            .description("Swift password")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SWIFT_TENANT = new PropertyDescriptor.Builder()
            .name("swift-tenant")
            .displayName("Tenant (Keystone)")
            .description("If Keystone requires tenant/project name")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONTAINER = new PropertyDescriptor.Builder()
            .name("swift-container")
            .displayName("Container")
            .description("Target container")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PREFIX = new PropertyDescriptor.Builder()
            .name("swift-prefix")
            .displayName("Prefix")
            .description("Optional prefix filter")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PAGE_SIZE = new PropertyDescriptor.Builder()
            .name("page-size")
            .displayName("Page Size")
            .description("Number of objects per request")
            .required(false)
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor MIN_AGE = new PropertyDescriptor.Builder()
            .name("min-age")
            .displayName("Min Age")
            .description("Skip objects younger than this")
            .defaultValue("0 sec")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_AGE = new PropertyDescriptor.Builder()
            .name("max-age")
            .displayName("Max Age")
            .description("Skip objects older than this")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_SIZE = new PropertyDescriptor.Builder()
            .name("max-file-size")
            .displayName("Max File Size")
            .description("Skip objects larger than this size (e.g. 100 MB)")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final AllowableValue TRACK_TIMESTAMP = new AllowableValue("timestamp", "Timestamp", "Track by lastModified");
    public static final AllowableValue TRACK_ENTITY = new AllowableValue("entity", "Entity", "Track each object name");
    public static final PropertyDescriptor TRACKING_STRATEGY = new PropertyDescriptor.Builder()
            .name("tracking-strategy")
            .displayName("Tracking Strategy")
            .description("Use 'timestamp' or 'entity' tracking")
            .required(true)
            .allowableValues(TRACK_TIMESTAMP, TRACK_ENTITY)
            .defaultValue(TRACK_TIMESTAMP.getValue())
            .build();

    private static final String STATE_TIMESTAMP = "swift.timestamp";
    private static final String STATE_KEY_PREFIX = "swift.key-";
    private static final String STATE_ENTITY_KEY_PREFIX = "swift.entity-";

    private volatile Account swiftAccount;
    private volatile Container swiftContainer;

    private volatile long minAgeMs;
    private volatile Long maxAgeMs;
    private volatile long maxFileSize;
    private volatile int pageSize;
    private volatile TrackingMode trackingMode;

    private final AtomicReference<ListingState> listingState = new AtomicReference<>(ListingState.empty());

    enum TrackingMode { TIMESTAMP, ENTITY }

    static class ListingState {
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

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SWIFT_AUTH_URL);
        descriptors.add(AUTH_METHOD);
        descriptors.add(SWIFT_USERNAME);
        descriptors.add(SWIFT_PASSWORD);
        descriptors.add(SWIFT_TENANT);
        descriptors.add(CONTAINER);
        descriptors.add(PREFIX);
        descriptors.add(PAGE_SIZE);
        descriptors.add(MIN_AGE);
        descriptors.add(MAX_AGE);
        descriptors.add(MAX_SIZE);
        descriptors.add(TRACKING_STRATEGY);
        return descriptors;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        ComponentLog logger = getLogger();

        try {
            restoreState(session);
        } catch (IOException e) {
            logger.error("State restore failed", e);
            context.yield();
            return;
        }

        if (!initializeSwiftConnection(context, logger)) {
            context.yield();
            return;
        }

        setupFilters(context);
        List<StoredObject> newObjects = fetchNewObjects(context, logger);
        if (newObjects.isEmpty()) {
            context.yield();
            return;
        }

        for (StoredObject object : newObjects) {
            FlowFile flowFile = session.create();
            flowFile = session.putAllAttributes(flowFile, buildAttributes(object, swiftContainer.getName()));
            session.transfer(flowFile, REL_SUCCESS);
        }

        try {
            updateState(session, newObjects);
        } catch (IOException e) {
            logger.error("State persist failed", e);
        }
    }

    private boolean initializeSwiftConnection(ProcessContext context, ComponentLog logger) {
        if (swiftAccount != null && swiftContainer != null) {
            return true;
        }
        try {
            swiftAccount = buildSwiftAccount(context);
            String containerName = context.getProperty(CONTAINER).getValue();
            swiftContainer = swiftAccount.getContainer(containerName);
            if (!swiftContainer.exists()) {
                logger.error("Container not found: {}", containerName);
                return false;
            }
        } catch (Exception ex) {
            logger.error("Swift connection failed", ex);
            return false;
        }
        return true;
    }

    private void setupFilters(ProcessContext context) {
        minAgeMs = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        maxAgeMs = context.getProperty(MAX_AGE).isSet() ? context.getProperty(MAX_AGE).asTimePeriod(TimeUnit.MILLISECONDS) : null;
        pageSize = context.getProperty(PAGE_SIZE).asInteger();
        PropertyValue maxSizeValue = context.getProperty(MAX_SIZE);
        maxFileSize = maxSizeValue.isSet() ? maxSizeValue.asDataSize(DataUnit.B).longValue() : Long.MAX_VALUE;
        String trackingValue = context.getProperty(TRACKING_STRATEGY).getValue();
        trackingMode = "entity".equalsIgnoreCase(trackingValue) ? TrackingMode.ENTITY : TrackingMode.TIMESTAMP;
    }

    private List<StoredObject> fetchNewObjects(ProcessContext context, ComponentLog logger) {
        List<StoredObject> objects = new ArrayList<>();
        String marker = null;
        String prefix = context.getProperty(PREFIX).getValue();
        if (prefix == null) {
            prefix = "";
        }
        long currentTime = System.currentTimeMillis();
        ListingState currentState = listingState.get();
        long lastTimestamp = currentState.timestamp;
        Set<String> timestampKeys = currentState.timestampKeys;
        Set<String> entityKeys = currentState.entityKeys;

        while (true) {
            Collection<StoredObject> page = swiftContainer.list(prefix, marker, pageSize);
            if (page.isEmpty()) {
                break;
            }
            for (StoredObject object : page) {
                try {
                    object.reload();
                } catch (Exception ex) {
                    logger.warn("Error reloading metadata, skipping: " + object.getName(), ex);
                    continue;
                }
                long lastModified = object.getLastModifiedAsDate() != null ? object.getLastModifiedAsDate().getTime() : 0L;
                if (object.getContentLength() > maxFileSize) {
                    continue;
                }
                if (!passesAge(lastModified, currentTime)) {
                    continue;
                }
                if (alreadyProcessed(object.getName(), lastModified, lastTimestamp, timestampKeys, entityKeys)) {
                    continue;
                }
                objects.add(object);
            }
            List<StoredObject> objectList = new ArrayList<>(page);
            marker = objectList.get(objectList.size() - 1).getName();
            if (objectList.size() < pageSize) {
                break;
            }
        }
        return objects;
    }

    private boolean alreadyProcessed(String objectName, long objectLastModified, long lastTimestamp,
                                     Set<String> timestampKeys, Set<String> entityKeys) {
        if (trackingMode == TrackingMode.ENTITY) {
            return entityKeys.contains(objectName);
        } else {
            return objectLastModified < lastTimestamp ||
                    (objectLastModified == lastTimestamp && timestampKeys.contains(objectName));
        }
    }

    private boolean passesAge(long lastModified, long currentTime) {
        if (lastModified <= 0L) {
            return true;
        }
        if (lastModified > (currentTime - minAgeMs)) {
            return false;
        }
        return maxAgeMs == null || lastModified >= (currentTime - maxAgeMs);
    }

    private Map<String, String> buildAttributes(StoredObject object, String containerName) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), object.getName());
        attributes.put("swift.container", containerName);
        attributes.put("swift.etag", object.getEtag() != null ? object.getEtag() : "");
        attributes.put("swift.length", Long.toString(object.getContentLength()));
        return attributes;
    }

    private Account buildSwiftAccount(ProcessContext context) {
        String authUrl = context.getProperty(SWIFT_AUTH_URL).getValue();
        String username = context.getProperty(SWIFT_USERNAME).getValue();
        String password = context.getProperty(SWIFT_PASSWORD).getValue();
        String tenant = context.getProperty(SWIFT_TENANT).getValue();
        String method = context.getProperty(AUTH_METHOD).getValue();

        AccountFactory factory = new AccountFactory()
                .setAuthUrl(authUrl)
                .setUsername(username)
                .setPassword(password);
        if ("KEYSTONE".equalsIgnoreCase(method)) {
            factory.setAuthenticationMethod(AuthenticationMethod.KEYSTONE);
            if (tenant != null && !tenant.isEmpty()) {
                factory.setTenantName(tenant);
            }
        } else {
            factory.setAuthenticationMethod(AuthenticationMethod.TEMPAUTH);
        }
        return factory.createAccount();
    }

    private void restoreState(ProcessSession session) throws IOException {
        StateMap stateMap = session.getState(Scope.CLUSTER);
        if (stateMap == null || stateMap.toMap().isEmpty()) {
            listingState.set(ListingState.empty());
            return;
        }
        long restoredTimestamp = 0L;
        Set<String> restoredTimestampKeys = new HashSet<>();
        Set<String> restoredEntityKeys = new HashSet<>();
        String tsValue = stateMap.get(STATE_TIMESTAMP);
        if (tsValue != null) {
            restoredTimestamp = Long.parseLong(tsValue);
        }
        for (Map.Entry<String, String> entry : stateMap.toMap().entrySet()) {
            if (entry.getKey().startsWith(STATE_KEY_PREFIX)) {
                restoredTimestampKeys.add(entry.getValue());
            } else if (entry.getKey().startsWith(STATE_ENTITY_KEY_PREFIX)) {
                restoredEntityKeys.add(entry.getValue());
            }
        }
        listingState.set(new ListingState(restoredTimestamp, restoredTimestampKeys, restoredEntityKeys));
    }

    private void updateState(ProcessSession session, List<StoredObject> newObjects) throws IOException {
        ListingState currentState = listingState.get();
        long lastTimestamp = currentState.timestamp;
        Set<String> timestampKeys = new HashSet<>(currentState.timestampKeys);
        Set<String> entityKeys = new HashSet<>(currentState.entityKeys);
        long newTimestamp = lastTimestamp;
        Set<String> newTimestampKeys = new HashSet<>(timestampKeys);

        for (StoredObject object : newObjects) {
            long lastModified = object.getLastModifiedAsDate() != null ? object.getLastModifiedAsDate().getTime() : 0L;
            if (trackingMode == TrackingMode.ENTITY) {
                entityKeys.add(object.getName());
            } else {
                if (lastModified > newTimestamp) {
                    newTimestamp = lastModified;
                    newTimestampKeys.clear();
                    newTimestampKeys.add(object.getName());
                } else if (lastModified == newTimestamp) {
                    newTimestampKeys.add(object.getName());
                }
            }
        }
        if (trackingMode == TrackingMode.ENTITY) {
            persistEntityState(session, entityKeys);
            listingState.set(new ListingState(lastTimestamp, timestampKeys, entityKeys));
        } else {
            persistTimestampState(session, newTimestamp, newTimestampKeys, entityKeys);
            listingState.set(new ListingState(newTimestamp, newTimestampKeys, entityKeys));
        }
    }

    private void persistTimestampState(ProcessSession session, long timestamp, Set<String> timestampKeys, Set<String> entityKeys) throws IOException {
        Map<String, String> newState = new HashMap<>();
        newState.put(STATE_TIMESTAMP, String.valueOf(timestamp));
        int i = 0;
        for (String key : timestampKeys) {
            newState.put(STATE_KEY_PREFIX + i, key);
            i++;
        }
        int j = 0;
        for (String key : entityKeys) {
            newState.put(STATE_ENTITY_KEY_PREFIX + j, key);
            j++;
        }
        session.setState(newState, Scope.CLUSTER);
    }

    private void persistEntityState(ProcessSession session, Set<String> entityKeys) throws IOException {
        Map<String, String> newState = new HashMap<>();
        int i = 0;
        for (String key : entityKeys) {
            newState.put(STATE_ENTITY_KEY_PREFIX + i, key);
            i++;
        }
        session.setState(newState, Scope.CLUSTER);
    }
}
