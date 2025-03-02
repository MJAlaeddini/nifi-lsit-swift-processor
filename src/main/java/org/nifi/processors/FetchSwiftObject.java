package org.nifi.processors;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.instructions.DownloadInstructions;
import org.javaswift.joss.model.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;

@SupportsBatching
@TriggerSerially
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"OpenStack", "Swift", "Fetch", "ObjectStorage"})
@CapabilityDescription("Fetches the contents of an Object from OpenStack Swift, writing the content to the FlowFile. "
        + "Similar to FetchS3Object.")
@WritesAttributes({
        @WritesAttribute(attribute = "swift.container", description = "The Swift container name"),
        @WritesAttribute(attribute = "swift.filename", description = "The name/key of the object in Swift"),
        @WritesAttribute(attribute = "swift.etag", description = "ETag of the object if present"),
        @WritesAttribute(attribute = "swift.length", description = "Size of the object in bytes"),
        @WritesAttribute(attribute = "swift.lastModified", description = "Last-Modified date of the object, if available"),
        @WritesAttribute(attribute = "swift.fetch.error", description = "Error message if fetching fails"),
        @WritesAttribute(attribute = "mime.type", description = "If available, the content-type from Swift metadata")
})
@SeeAlso({})
@org.apache.nifi.annotation.configuration.DefaultSchedule(
        strategy = SchedulingStrategy.TIMER_DRIVEN,
        period = "0 sec"
)
public class FetchSwiftObject extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles successfully fetched from Swift")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to be fetched from Swift")
            .build();

    public static final AllowableValue TEMPAUTH = new AllowableValue("TEMPAUTH", "TempAuth", "v1 style auth");
    public static final AllowableValue KEYSTONE = new AllowableValue("KEYSTONE", "Keystone", "Keystone style auth");

    public static final PropertyDescriptor SWIFT_AUTH_METHOD = new PropertyDescriptor.Builder()
            .name("swift-auth-method")
            .displayName("Swift Auth Method")
            .description("Choose TempAuth or Keystone")
            .required(true)
            .allowableValues(TEMPAUTH, KEYSTONE)
            .defaultValue(TEMPAUTH.getValue())
            .build();

    public static final PropertyDescriptor SWIFT_AUTH_URL = new PropertyDescriptor.Builder()
            .name("swift-auth-url")
            .displayName("Swift Auth URL")
            .description("Authentication endpoint for Swift")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SWIFT_USERNAME = new PropertyDescriptor.Builder()
            .name("swift-username")
            .displayName("Swift Username")
            .description("Username or tenant:username for Swift")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SWIFT_PASSWORD = new PropertyDescriptor.Builder()
            .name("swift-password")
            .displayName("Swift Password")
            .description("Password for Swift")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SWIFT_TENANT = new PropertyDescriptor.Builder()
            .name("swift-tenant")
            .displayName("Swift Tenant (Keystone)")
            .description("Optional tenant/project name if Keystone is used")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONTAINER = new PropertyDescriptor.Builder()
            .name("swift-container")
            .displayName("Container")
            .description("Name of the container in Swift")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OBJECT_NAME = new PropertyDescriptor.Builder()
            .name("swift-object-name")
            .displayName("Object Name")
            .description("Name/key of the object in Swift to fetch")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RANGE_START = new PropertyDescriptor.Builder()
            .name("range-start")
            .displayName("Range Start")
            .description("Byte offset to start reading from the object (inclusive)")
            .required(false)
            .defaultValue("0 B")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor RANGE_LENGTH = new PropertyDescriptor.Builder()
            .name("range-length")
            .displayName("Range Length")
            .description("Number of bytes to read from the object. If not set, reads until end.")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS;
    static {
        List<PropertyDescriptor> list = new ArrayList<>();
        list.add(SWIFT_AUTH_METHOD);
        list.add(SWIFT_AUTH_URL);
        list.add(SWIFT_USERNAME);
        list.add(SWIFT_PASSWORD);
        list.add(SWIFT_TENANT);
        list.add(CONTAINER);
        list.add(OBJECT_NAME);
        list.add(RANGE_START);
        list.add(RANGE_LENGTH);
        PROPERTY_DESCRIPTORS = Collections.unmodifiableList(list);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    public Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        // Add custom validations here if needed
        return super.customValidate(validationContext);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            long startNanos = System.nanoTime();

            flowFile = fetchObject(context, session, flowFile);

            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            flowFile = updateFlowFileAttributes(context, session, flowFile, elapsedMillis);

            session.transfer(flowFile, REL_SUCCESS);

        } catch (SwiftFetchException e) {
            getLogger().error("Swift fetch failed: {}", e.getMessage(), e);
            flowFile = session.putAttribute(flowFile, "swift.fetch.error", e.getMessage());
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    /**
     * Downloads the Swift object's content into the given FlowFile.
     */
    private FlowFile fetchObject(ProcessContext context, ProcessSession session, FlowFile flowFile)
            throws SwiftFetchException {
        final ComponentLog log = getLogger();

        String containerName = evaluateEL(context, flowFile, CONTAINER);
        String objectName = evaluateEL(context, flowFile, OBJECT_NAME);

        if (isEmpty(containerName) || isEmpty(objectName)) {
            throw new SwiftFetchException("Empty container or object name");
        }

        Account account = buildSwiftAccount(context);
        Container container = getValidatedContainer(account, containerName);
        StoredObject storedObject = getValidatedStoredObject(container, objectName);

        long rangeStart = parseRangeStart(context, flowFile);
        Long rangeLength = parseRangeLength(context, flowFile);
        DownloadInstructions instructions = buildDownloadInstructions(rangeStart, rangeLength);

        try (InputStream in = storedObject.downloadObjectAsInputStream(instructions)) {
            flowFile = session.importFrom(in, flowFile);
        } catch (FlowFileAccessException | IOException ex) {
            throw new SwiftFetchException("I/O error fetching object: " + ex.getMessage(), ex);
        } catch (Exception ex) {
            throw new SwiftFetchException("Exception while fetching object: " + ex.getMessage(), ex);
        }

        // Save minimal Swift info for next step
        Map<String, String> tempAttrs = new HashMap<>();
        tempAttrs.put("swift.temp.container", containerName);
        tempAttrs.put("swift.temp.objectName", objectName);
        flowFile = session.putAllAttributes(flowFile, tempAttrs);

        log.debug("Fetched object {} from container {}", objectName, containerName);
        return flowFile;
    }

    /**
     * Updates the FlowFile attributes with Swift metadata, sets MIME type, and reports Provenance.
     */
    private FlowFile updateFlowFileAttributes(ProcessContext context,
                                              ProcessSession session,
                                              FlowFile flowFile,
                                              long elapsedMillis) throws SwiftFetchException {

        final ComponentLog log = getLogger();

        // Retrieve container and object name from attributes
        String containerName = flowFile.getAttribute("swift.temp.container");
        String objectName = flowFile.getAttribute("swift.temp.objectName");

        // Rebuild Swift objects for metadata reload
        Account account = buildSwiftAccount(context);
        Container container = getValidatedContainer(account, containerName);
        StoredObject storedObject = getValidatedStoredObject(container, objectName);

        Map<String, String> finalAttributes = new HashMap<>();

        finalAttributes.put("swift.container", containerName);
        finalAttributes.put("swift.filename", objectName);

        try {
            storedObject.reload();
            if (!isEmpty(storedObject.getEtag())) {
                finalAttributes.put("swift.etag", storedObject.getEtag());
            }
            finalAttributes.put("swift.length", String.valueOf(storedObject.getContentLength()));

            Date lastModified = storedObject.getLastModifiedAsDate();
            if (lastModified != null) {
                finalAttributes.put("swift.lastModified", String.valueOf(lastModified.getTime()));
            }

            String contentType = storedObject.getContentType();
            if (!isEmpty(contentType)) {
                finalAttributes.put(CoreAttributes.MIME_TYPE.key(), contentType);
            }
        } catch (Exception ex) {
            log.warn("Could not reload metadata for object {}: {}", objectName, ex.getMessage(), ex);
        }

        flowFile = session.putAllAttributes(flowFile, finalAttributes);
        session.getProvenanceReporter().fetch(flowFile, storedObject.getPublicURL(), elapsedMillis);

        log.info("Successfully fetched {} from container {} in {} ms", objectName, containerName, elapsedMillis);
        return flowFile;
    }

    private Account buildSwiftAccount(ProcessContext context) throws SwiftFetchException {
        try {
            String authUrl = context.getProperty(SWIFT_AUTH_URL).getValue();
            String authMethod = context.getProperty(SWIFT_AUTH_METHOD).getValue();
            String username = context.getProperty(SWIFT_USERNAME).getValue();
            String password = context.getProperty(SWIFT_PASSWORD).getValue();
            String tenant = context.getProperty(SWIFT_TENANT).getValue();

            AccountFactory factory = new AccountFactory()
                    .setAuthUrl(authUrl)
                    .setUsername(username)
                    .setPassword(password);

            if ("KEYSTONE".equalsIgnoreCase(authMethod)) {
                factory.setAuthenticationMethod(AuthenticationMethod.KEYSTONE);
                if (!isEmpty(tenant)) {
                    factory.setTenantName(tenant);
                }
            } else {
                factory.setAuthenticationMethod(AuthenticationMethod.TEMPAUTH);
            }
            return factory.createAccount();

        } catch (Exception ex) {
            throw new SwiftFetchException("Swift account creation failed: " + ex.getMessage(), ex);
        }
    }

    private Container getValidatedContainer(Account account, String containerName) throws SwiftFetchException {
        try {
            Container container = account.getContainer(containerName);
            if (!container.exists()) {
                throw new SwiftFetchException("Container not found: " + containerName);
            }
            return container;
        } catch (Exception ex) {
            throw new SwiftFetchException("Failed to connect to container: " + ex.getMessage(), ex);
        }
    }

    private StoredObject getValidatedStoredObject(Container container, String objectName) throws SwiftFetchException {
        StoredObject storedObject = container.getObject(objectName);
        try {
            if (!storedObject.exists()) {
                throw new SwiftFetchException("Object not found: " + objectName);
            }
        } catch (Exception ex) {
            throw new SwiftFetchException("Error checking object existence: " + ex.getMessage(), ex);
        }
        return storedObject;
    }

    private DownloadInstructions buildDownloadInstructions(long start, Long length) {
        DownloadInstructions instructions = new DownloadInstructions();
        if (start < 0) {
            start = 0;
        }
        if (length != null && length > 0) {
            long end = (start > 0) ? (start + length - 1) : (length - 1);
            instructions.setRange(new SwiftByteRange(start, end));
        } else if (start > 0) {
            instructions.setRange(new SwiftByteRange(start, -1));
        }
        return instructions;
    }

    private long parseRangeStart(ProcessContext context, FlowFile flowFile) {
        if (!context.getProperty(RANGE_START).isSet()) {
            return 0L;
        }
        return context.getProperty(RANGE_START)
                .evaluateAttributeExpressions(flowFile)
                .asDataSize(DataUnit.B)
                .longValue();
    }

    private Long parseRangeLength(ProcessContext context, FlowFile flowFile) {
        if (!context.getProperty(RANGE_LENGTH).isSet()) {
            return null;
        }
        return context.getProperty(RANGE_LENGTH)
                .evaluateAttributeExpressions(flowFile)
                .asDataSize(DataUnit.B)
                .longValue();
    }

    private String evaluateEL(ProcessContext context, FlowFile flowFile, PropertyDescriptor descriptor) {
        return context.getProperty(descriptor).evaluateAttributeExpressions(flowFile).getValue();
    }

    private boolean isEmpty(String val) {
        return val == null || val.isEmpty();
    }

    /**
     * Custom exception to unify error handling in fetch operations.
     */
    private static class SwiftFetchException extends RuntimeException {
        SwiftFetchException(String message) {
            super(message);
        }
        SwiftFetchException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
