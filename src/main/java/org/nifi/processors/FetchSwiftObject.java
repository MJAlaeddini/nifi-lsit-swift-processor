package org.nifi.processors;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.javaswift.joss.model.StoredObject;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@SupportsBatching
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"OpenStack", "Swift", "Fetch", "ObjectStorage"})
@CapabilityDescription("Fetches the contents of an object from OpenStack Swift, writing the content to the FlowFile.")
@WritesAttributes({
        @WritesAttribute(attribute = "swift.container", description = "The Swift container name"),
        @WritesAttribute(attribute = "swift.filename", description = "The object name in Swift"),
        @WritesAttribute(attribute = "swift.etag", description = "ETag of the object if present"),
        @WritesAttribute(attribute = "swift.length", description = "Size of the object in bytes"),
        @WritesAttribute(attribute = "swift.lastModified", description = "Last-Modified date of the object"),
        @WritesAttribute(attribute = "swift.fetch.error", description = "Error message if fetching fails"),
        @WritesAttribute(attribute = "mime.type", description = "Content-Type from Swift metadata if available")
})
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "0 sec")
public class FetchSwiftObject extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles successfully fetched from Swift")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to be fetched from Swift")
            .build();

    public static final PropertyDescriptor CONTAINER = new PropertyDescriptor.Builder()
            .name("swift-container")
            .displayName("Container")
            .description("Name of the container in Swift.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OBJECT_NAME = new PropertyDescriptor.Builder()
            .name("swift-object-name")
            .displayName("Object Name")
            .description("Name/key of the object in Swift to fetch.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS;

    private SwiftContainerFactory swiftContainerFactory;
    private SwiftService swiftService;

    static {
        List<PropertyDescriptor> list = new ArrayList<>();
        list.addAll(SwiftContainerFactory.SWIFT_COMMON_PROPERTIES); // پارامترهای عمومی اتصال Swift
        list.add(CONTAINER);
        list.add(OBJECT_NAME);
        PROPERTY_DESCRIPTORS = Collections.unmodifiableList(list);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE));
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        this.swiftContainerFactory = new SwiftContainerFactory(context);
        this.swiftService = new SwiftService(context);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        ComponentLog logger = getLogger();

        String containerName = context.getProperty(CONTAINER).evaluateAttributeExpressions(flowFile).getValue();
        String objectName = context.getProperty(OBJECT_NAME).evaluateAttributeExpressions(flowFile).getValue();

        if (containerName == null || containerName.isEmpty() || objectName == null || objectName.isEmpty()) {
            flowFile = session.putAttribute(flowFile, "swift.fetch.error", "Container or ObjectName is empty");
            session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        long startNanos = System.nanoTime();
        try {
            try (InputStream in = swiftService.downloadObject(containerName, objectName)) {
                flowFile = session.importFrom(in, flowFile);
            }

            StoredObject so = swiftService.getContainer(containerName).getObject(objectName);
            so.reload();

            Map<String, String> attrs = SwiftAttributeBuilder.fromStoredObject(containerName, objectName, so);
            flowFile = session.putAllAttributes(flowFile, attrs);

            long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
            session.getProvenanceReporter().fetch(flowFile, so.getPublicURL(), elapsedMillis);

            session.transfer(flowFile, REL_SUCCESS);
            logger.info("Successfully fetched object {} from container {} in {} ms",
                    new Object[]{objectName, containerName, elapsedMillis});

        } catch (SwiftException | IOException | FlowFileAccessException e) {
            logger.error("Error fetching object from Swift: " + e.getMessage(), e);
            flowFile = session.putAttribute(flowFile, "swift.fetch.error", e.getMessage());
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
