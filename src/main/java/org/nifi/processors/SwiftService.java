package org.nifi.processors;

import org.apache.nifi.processor.ProcessContext;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;

import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class SwiftService {

    private final ProcessContext context;
    private final AtomicReference<Account> cachedAccount = new AtomicReference<>();

    public SwiftService(ProcessContext context) {
        this.context = context;
    }

    private Account getOrCreateAccount() throws SwiftException {
        Account account = cachedAccount.get();
        if (account != null) {
            return account;
        }
        account = buildSwiftAccount();
        cachedAccount.set(account);
        return account;
    }

    private Account buildSwiftAccount() throws SwiftException {
        try {
            String authUrl = context.getProperty(SwiftContainerFactory.SWIFT_AUTH_URL).getValue();
            String authMethod = context.getProperty(SwiftContainerFactory.SWIFT_AUTH_METHOD).getValue();
            String username = context.getProperty(SwiftContainerFactory.SWIFT_USERNAME).getValue();
            String password = context.getProperty(SwiftContainerFactory.SWIFT_PASSWORD).getValue();
            String tenant   = context.getProperty(SwiftContainerFactory.SWIFT_TENANT).getValue();

            AccountFactory factory = new AccountFactory()
                    .setAuthUrl(authUrl)
                    .setUsername(username)
                    .setPassword(password);

            if ("KEYSTONE".equalsIgnoreCase(authMethod)) {
                factory.setAuthenticationMethod(AuthenticationMethod.KEYSTONE);
                if (tenant != null && !tenant.isEmpty()) {
                    factory.setTenantName(tenant);
                }
            } else {
                factory.setAuthenticationMethod(AuthenticationMethod.TEMPAUTH);
            }

            return factory.createAccount();
        } catch (Exception e) {
            throw new SwiftException("Could not create Swift account: " + e.getMessage(), e);
        }
    }

    public Container getContainer(String containerName) throws SwiftException {
        try {
            Container container = getOrCreateAccount().getContainer(containerName);
            if (!container.exists()) {
                throw new SwiftException("Container does not exist: " + containerName);
            }
            return container;
        } catch (Exception ex) {
            if (ex.getMessage() != null && ex.getMessage().contains("token expired")) {
                cachedAccount.set(null);
                // Retry
                Container container = getOrCreateAccount().getContainer(containerName);
                if (!container.exists()) {
                    throw new SwiftException("Container does not exist: " + containerName);
                }
                return container;
            }
            throw new SwiftException("Error getting container: " + ex.getMessage(), ex);
        }
    }


    public List<StoredObject> listObjects(Container container, String prefix, String marker, int pageSize) {
        Collection<StoredObject> page = container.list(prefix, marker, pageSize);
        return new ArrayList<>(page);
    }

    public InputStream downloadObject(String containerName, String objectName) throws SwiftException {
        Container container = getContainer(containerName);
        StoredObject so = container.getObject(objectName);
        if (!so.exists()) {
            throw new SwiftException("Object not found: " + objectName);
        }
        return so.downloadObjectAsInputStream();
    }
}
