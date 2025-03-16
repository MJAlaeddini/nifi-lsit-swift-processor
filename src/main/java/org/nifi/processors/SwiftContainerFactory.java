package org.nifi.processors;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SwiftContainerFactory {

    public static final AllowableValue TEMPAUTH = new AllowableValue("TEMPAUTH", "TempAuth", "v1 style auth");
    public static final AllowableValue KEYSTONE = new AllowableValue("KEYSTONE", "Keystone", "Keystone style auth");

    public static final PropertyDescriptor SWIFT_AUTH_METHOD = new PropertyDescriptor.Builder()
            .name("swift-auth-method")
            .displayName("Swift Auth Method")
            .description("Authentication method to use for Swift: TempAuth or Keystone.")
            .required(true)
            .allowableValues(TEMPAUTH, KEYSTONE)
            .defaultValue(TEMPAUTH.getValue())
            .build();

    public static final PropertyDescriptor SWIFT_AUTH_URL = new PropertyDescriptor.Builder()
            .name("swift-auth-url")
            .displayName("Swift Auth URL")
            .description("Authentication endpoint for Swift.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SWIFT_USERNAME = new PropertyDescriptor.Builder()
            .name("swift-username")
            .displayName("Swift Username")
            .description("Username or tenant:username for Swift.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SWIFT_PASSWORD = new PropertyDescriptor.Builder()
            .name("swift-password")
            .displayName("Swift Password")
            .description("Password for Swift.")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SWIFT_TENANT = new PropertyDescriptor.Builder()
            .name("swift-tenant")
            .displayName("Swift Tenant (Keystone)")
            .description("Optional tenant/project name if Keystone is used.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final List<PropertyDescriptor> SWIFT_COMMON_PROPERTIES;

    static {
        List<PropertyDescriptor> list = new ArrayList<>();
        list.add(SWIFT_AUTH_METHOD);
        list.add(SWIFT_AUTH_URL);
        list.add(SWIFT_USERNAME);
        list.add(SWIFT_PASSWORD);
        list.add(SWIFT_TENANT);
        SWIFT_COMMON_PROPERTIES = Collections.unmodifiableList(list);
    }

    private final ProcessContext context;

    public SwiftContainerFactory(ProcessContext context) {
        this.context = context;
    }

    public List<PropertyDescriptor> getCommonProperties() {
        return SWIFT_COMMON_PROPERTIES;
    }

    public SwiftService createSwiftService() {
        return new SwiftService(context);
    }
}
