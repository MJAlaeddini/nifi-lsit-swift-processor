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
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;

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

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles successfully fetched from Swift")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to be fetched from Swift")
            .build();

    // Auth method
    public static final AllowableValue TEMPAUTH = new AllowableValue("TEMPAUTH", "TempAuth", "v1 style auth");
    public static final AllowableValue KEYSTONE = new AllowableValue("KEYSTONE", "Keystone", "Keystone style auth");

    // Properties
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
            .expressionLanguageSupported(org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OBJECT_NAME = new PropertyDescriptor.Builder()
            .name("swift-object-name")
            .displayName("Object Name")
            .description("Name/key of the object in Swift to fetch")
            .required(true)
            .expressionLanguageSupported(org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RANGE_START = new PropertyDescriptor.Builder()
            .name("range-start")
            .displayName("Range Start")
            .description("Byte offset to start reading from the object (inclusive)")
            .required(false)
            .defaultValue("0 B")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .expressionLanguageSupported(org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor RANGE_LENGTH = new PropertyDescriptor.Builder()
            .name("range-length")
            .displayName("Range Length")
            .description("Number of bytes to read from the object. If not set, reads until end.")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .expressionLanguageSupported(org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    // Statically define property list
    static final List<PropertyDescriptor> propDescriptors;

    static {
        final List<PropertyDescriptor> _temp = new ArrayList<>();
        _temp.add(SWIFT_AUTH_METHOD);
        _temp.add(SWIFT_AUTH_URL);
        _temp.add(SWIFT_USERNAME);
        _temp.add(SWIFT_PASSWORD);
        _temp.add(SWIFT_TENANT);
        _temp.add(CONTAINER);
        _temp.add(OBJECT_NAME);
        _temp.add(RANGE_START);
        _temp.add(RANGE_LENGTH);
        propDescriptors = Collections.unmodifiableList(_temp);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        return rels;
    }

    @Override
    public Collection<ValidationResult> customValidate(ValidationContext context) {
        // No special validations yet, but you can add checks if needed
        return new ArrayList<>(super.customValidate(context));
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        final String containerName = context.getProperty(CONTAINER).evaluateAttributeExpressions(flowFile).getValue();
        final String objectName = context.getProperty(OBJECT_NAME).evaluateAttributeExpressions(flowFile).getValue();

        if (containerName == null || containerName.isEmpty() || objectName == null || objectName.isEmpty()) {
            logger.error("Container or Object Name is invalid");
            flowFile = session.putAttribute(flowFile, "swift.fetch.error", "Empty container/object name");
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        // Build the account/container
        Account account;
        Container container;
        try {
            account = buildAccount(context);
            container = account.getContainer(containerName);
            if (!container.exists()) {
                logger.error("Container {} does not exist", containerName);
                flowFile = session.putAttribute(flowFile, "swift.fetch.error", "Container not found");
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
        } catch (Exception ex) {
            logger.error("Failed to connect to Swift", ex);
            flowFile = session.putAttribute(flowFile, "swift.fetch.error", "Swift connection failed: " + ex.getMessage());
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        // Get range config
        long rangeStart = 0L;
        Long rangeLength = null;
        try {
            if (context.getProperty(RANGE_START).isSet()) {
                rangeStart = context.getProperty(RANGE_START).evaluateAttributeExpressions(flowFile)
                        .asDataSize(DataUnit.B).longValue();
            }
            if (context.getProperty(RANGE_LENGTH).isSet()) {
                rangeLength = context.getProperty(RANGE_LENGTH).evaluateAttributeExpressions(flowFile)
                        .asDataSize(DataUnit.B).longValue();
            }
        } catch (Exception ex) {
            logger.error("Invalid range settings", ex);
            flowFile = session.putAttribute(flowFile, "swift.fetch.error", "Invalid range: " + ex.getMessage());
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        // Try to fetch the object
        StoredObject so = container.getObject(objectName);
        try {
            if (!so.exists()) {
                logger.warn("Object {} in container {} does not exist", new Object[]{objectName, containerName});
                flowFile = session.putAttribute(flowFile, "swift.fetch.error", "Object not found");
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
        } catch (Exception ex) {
            logger.error("Error checking existence for object {}", new Object[]{objectName, ex});
            flowFile = session.putAttribute(flowFile, "swift.fetch.error", "Error checking existence: " + ex.getMessage());
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        DownloadInstructions dInstr = new DownloadInstructions();

        // If both rangeStart & rangeLength set => read from 'rangeStart' to 'rangeStart + rangeLength - 1'
        if (rangeStart >= 0 && rangeLength != null && rangeLength > 0) {
            long end = rangeStart + rangeLength - 1;
            MyByteRange myRange = new MyByteRange(rangeStart, end);
            dInstr.setRange(myRange);
        } else if (rangeStart > 0) {
            // pass -1 as end => in MyByteRange => means "till the end"
            MyByteRange myRange = new MyByteRange(rangeStart, -1);
            dInstr.setRange(myRange);
        } else if (rangeLength != null && rangeLength > 0) {
            MyByteRange myRange = new MyByteRange(0, rangeLength - 1);
            dInstr.setRange(myRange);
        }

        final long startNanos = System.nanoTime();

        try (InputStream in = so.downloadObjectAsInputStream(dInstr)) {
            flowFile = session.importFrom(in, flowFile);
        } catch (FlowFileAccessException ffae) {
            logger.error("Failed to download object: {}", new Object[]{ffae.getMessage(), ffae});
            flowFile = session.putAttribute(flowFile, "swift.fetch.error", "FlowFileAccessException: " + ffae.getMessage());
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        } catch (IOException ioex) {
            logger.error("IO error fetching object: {}", new Object[]{ioex.getMessage(), ioex});
            flowFile = session.putAttribute(flowFile, "swift.fetch.error", "IOException: " + ioex.getMessage());
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        } catch (Exception ex) {
            logger.error("Error fetching object: {}", new Object[]{ex.getMessage(), ex});
            flowFile = session.putAttribute(flowFile, "swift.fetch.error", "Exception: " + ex.getMessage());
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        // Update attributes with metadata
        Map<String, String> attrs = new HashMap<>();
        attrs.put("swift.container", containerName);
        attrs.put("swift.filename", objectName);

        // Reload metadata
        try {
            so.reload(); // get updated metadata
            String etag = so.getEtag();
            if (etag != null) {
                attrs.put("swift.etag", etag);
            }
            long length = so.getContentLength();
            attrs.put("swift.length", String.valueOf(length));

            Date lm = so.getLastModifiedAsDate();
            if (lm != null) {
                attrs.put("swift.lastModified", String.valueOf(lm.getTime()));
            }

            // If content type is known
            String contentType = so.getContentType();
            if (contentType != null && !contentType.isEmpty()) {
                attrs.put(CoreAttributes.MIME_TYPE.key(), contentType);
            }
        } catch (Exception ex) {
            logger.warn("Failed to reload metadata after fetch: {}", new Object[]{ex.getMessage(), ex});
        }

        long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

        flowFile = session.putAllAttributes(flowFile, attrs);
        session.getProvenanceReporter().fetch(flowFile, so.getPublicURL(), millis);
        session.transfer(flowFile, REL_SUCCESS);
        logger.info("Successfully fetched Swift object {} from container {} in {} ms", new Object[]{objectName, containerName, millis});
    }

    private Account buildAccount(ProcessContext context) {
        String authUrl = context.getProperty(SWIFT_AUTH_URL).getValue();
        String method = context.getProperty(SWIFT_AUTH_METHOD).getValue();
        String user = context.getProperty(SWIFT_USERNAME).getValue();
        String pass = context.getProperty(SWIFT_PASSWORD).getValue();
        String tenant = context.getProperty(SWIFT_TENANT).getValue();

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
}
