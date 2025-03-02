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
import org.apache.nifi.components.*;
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
@SeeAlso({})
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

    private static final Set<Relationship> RELS = Collections.singleton(REL_SUCCESS);

    // Authentication props
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

    // Container / listing props
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

    // Age / size props
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

    // Tracking props
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

    // State keys
    private static final String STATE_TIMESTAMP = "swift.timestamp";
    private static final String STATE_KEY_PREFIX = "swift.key-"; // used in timestamp mode
    private static final String STATE_ENTITY_KEY_PREFIX = "swift.entity-"; // used in entity mode

    private volatile Account swiftAccount;
    private volatile Container swiftContainer;

    private volatile long minAgeMs;
    private volatile Long maxAgeMs;
    private volatile long maxFileSize = Long.MAX_VALUE;
    private volatile int pageSize;
    private volatile TrackingState trackingState;

    private final AtomicReference<ListingState> listingState = new AtomicReference<>(ListingState.empty());

    static class ListingState {
        final long timestamp;   // used in Timestamp mode
        final Set<String> latestKeys; // for same timestamp
        final Set<String> entityKeys; // used in Entity mode
        ListingState(long ts, Set<String> keys, Set<String> entities) {
            this.timestamp = ts;
            this.latestKeys = keys;
            this.entityKeys = entities;
        }
        static ListingState empty() {
            return new ListingState(0L, Collections.emptySet(), Collections.emptySet());
        }
    }

    enum TrackingState {
        TIMESTAMP,
        ENTITY
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(SWIFT_AUTH_URL);
        props.add(AUTH_METHOD);
        props.add(SWIFT_USERNAME);
        props.add(SWIFT_PASSWORD);
        props.add(SWIFT_TENANT);
        props.add(CONTAINER);
        props.add(PREFIX);
        props.add(PAGE_SIZE);
        props.add(MIN_AGE);
        props.add(MAX_AGE);
        props.add(MAX_SIZE);
        props.add(TRACKING_STRATEGY);
        return props;
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

        if (swiftAccount == null || swiftContainer == null) {
            try {
                swiftAccount = buildSwiftAccount(context);
                String containerName = context.getProperty(CONTAINER).getValue();
                swiftContainer = swiftAccount.getContainer(containerName);
                if (!swiftContainer.exists()) {
                    logger.error("Container not found: {}", containerName);
                    context.yield();
                    return;
                }
            } catch (Exception ex) {
                logger.error("Swift connection failed", ex);
                context.yield();
                return;
            }
        }

        minAgeMs = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        maxAgeMs = context.getProperty(MAX_AGE).isSet()
                ? context.getProperty(MAX_AGE).asTimePeriod(TimeUnit.MILLISECONDS)
                : null;
        pageSize = context.getProperty(PAGE_SIZE).asInteger();
        if (context.getProperty(MAX_SIZE).isSet()) {
            maxFileSize = context.getProperty(MAX_SIZE).asDataSize(DataUnit.B).longValue();
        } else {
            maxFileSize = Long.MAX_VALUE;
        }
        trackingState = "entity".equalsIgnoreCase(context.getProperty(TRACKING_STRATEGY).getValue())
                ? TrackingState.ENTITY
                : TrackingState.TIMESTAMP;

        String prefixVal = context.getProperty(PREFIX).getValue();
        if (prefixVal == null) prefixVal = "";

        // current state
        long now = System.currentTimeMillis();
        long lastTs = listingState.get().timestamp;
        Set<String> lastKeys = listingState.get().latestKeys;
        Set<String> entityKeys = listingState.get().entityKeys;

        long newMaxTs = lastTs;
        Set<String> newMaxKeys = new HashSet<>(lastKeys); // for the same TS
        Set<String> newEntityKeys = new HashSet<>(entityKeys);

        List<StoredObject> discovered = new ArrayList<>();
        String marker = null;
        boolean done = false;

        while (!done) {
            // list objects
            Collection<StoredObject> cObjects = swiftContainer.list(prefixVal, marker, pageSize);
            List<StoredObject> objects = new ArrayList<>(cObjects);
            if (objects.isEmpty()) {
                done = true;
                break;
            }
            for (StoredObject so : objects) {
                // Get actual metadata
                so.reload();
                Date lm = so.getLastModifiedAsDate();
                long lastMod = (lm != null) ? lm.getTime() : 0L;
                if (so.getLastModifiedAsDate() != null) {
                    lastMod = so.getLastModifiedAsDate().getTime();
                }
                long size = so.getContentLength();

                // If max size is set
                if (size > maxFileSize) {
                    continue;
                }
                // Age filter
                if (!passesAgeFilter(lastMod, now)) {
                    continue;
                }

                // Skip if already listed in entity tracking
                if (trackingState == TrackingState.ENTITY) {
                    if (entityKeys.contains(so.getName())) {
                        continue;
                    }
                } else {
                    // Timestamp tracking
                    if (lastMod < lastTs) {
                        continue;
                    }
                    if (lastMod == lastTs && lastKeys.contains(so.getName())) {
                        continue;
                    }
                }

                discovered.add(so);

                // Update new state
                if (trackingState == TrackingState.ENTITY) {
                    newEntityKeys.add(so.getName());
                } else {
                    if (lastMod > newMaxTs) {
                        newMaxTs = lastMod;
                        newMaxKeys.clear();
                        newMaxKeys.add(so.getName());
                    } else if (lastMod == newMaxTs) {
                        newMaxKeys.add(so.getName());
                    }
                }
            }
            marker = objects.get(objects.size() - 1).getName();
            if (objects.size() < pageSize) {
                done = true;
            }
        }

        // create FlowFiles
        for (StoredObject so : discovered) {
            FlowFile ff = session.create();
            ff = session.putAllAttributes(ff, buildAttributes(so, swiftContainer.getName()));
            session.transfer(ff, REL_SUCCESS);
        }

        // persist state
        if (!discovered.isEmpty()) {
            try {
                if (trackingState == TrackingState.ENTITY) {
                    persistEntityState(session, newEntityKeys);
                    listingState.set(new ListingState(lastTs, lastKeys, newEntityKeys));
                } else {
                    persistTimestampState(session, newMaxTs, newMaxKeys, newEntityKeys);
                    listingState.set(new ListingState(newMaxTs, newMaxKeys, newEntityKeys));
                }
            } catch (IOException e) {
                logger.error("State persist failed", e);
            }
        } else {
            context.yield();
        }
    }

    private boolean passesAgeFilter(long lastModified, long now) {
        if (lastModified <= 0L) {
            // if object has no valid lastModified, skip or treat as old
            return true;
        }
        if (lastModified > (now - minAgeMs)) return false;
        if (maxAgeMs != null && lastModified < (now - maxAgeMs)) return false;
        return true;
    }

    private Map<String, String> buildAttributes(StoredObject so, String containerName) {
        Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.FILENAME.key(), so.getName());
        attrs.put("swift.container", containerName);
        attrs.put("swift.etag", so.getEtag() != null ? so.getEtag() : "");
        attrs.put("swift.length", Long.toString(so.getContentLength()));
        return attrs;
    }

    private Account buildSwiftAccount(ProcessContext context) {
        String authUrl = context.getProperty(SWIFT_AUTH_URL).getValue();
        String user = context.getProperty(SWIFT_USERNAME).getValue();
        String pass = context.getProperty(SWIFT_PASSWORD).getValue();
        String tenant = context.getProperty(SWIFT_TENANT).getValue();
        String method = context.getProperty(AUTH_METHOD).getValue();

        AccountFactory factory = new AccountFactory()
                .setAuthUrl(authUrl)
                .setUsername(user)
                .setPassword(pass);

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
        StateMap map = session.getState(Scope.CLUSTER);
        if (map == null || map.toMap().isEmpty()) {
            listingState.set(ListingState.empty());
            return;
        }
        long ts = 0L;
        Set<String> lastKeys = new HashSet<>();
        Set<String> entityKeys = new HashSet<>();

        String tsVal = map.get(STATE_TIMESTAMP);
        if (tsVal != null) {
            ts = Long.parseLong(tsVal);
        }
        // timestamp keys
        for (Map.Entry<String, String> e : map.toMap().entrySet()) {
            if (e.getKey().startsWith(STATE_KEY_PREFIX)) {
                lastKeys.add(e.getValue());
            }
        }
        // entity keys
        for (Map.Entry<String, String> e : map.toMap().entrySet()) {
            if (e.getKey().startsWith(STATE_ENTITY_KEY_PREFIX)) {
                entityKeys.add(e.getValue());
            }
        }
        listingState.set(new ListingState(ts, lastKeys, entityKeys));
    }

    private void persistTimestampState(ProcessSession session, long ts, Set<String> keys, Set<String> entityKeys) throws IOException {
        Map<String, String> newState = new HashMap<>();
        newState.put(STATE_TIMESTAMP, String.valueOf(ts));
        int i = 0;
        for (String k : keys) {
            newState.put(STATE_KEY_PREFIX + i, k);
            i++;
        }
        // also persist entityKeys (if user switched from entity to timestamp at runtime, or vice versa)
        int j = 0;
        for (String ent : entityKeys) {
            newState.put(STATE_ENTITY_KEY_PREFIX + j, ent);
            j++;
        }
        session.setState(newState, Scope.CLUSTER);
    }

    private void persistEntityState(ProcessSession session, Set<String> entityKeys) throws IOException {
        // we do not rely on timestamp in entity mode
        Map<String, String> newState = new HashMap<>();
        int i = 0;
        for (String ent : entityKeys) {
            newState.put(STATE_ENTITY_KEY_PREFIX + i, ent);
            i++;
        }
        session.setState(newState, Scope.CLUSTER);
    }
}
