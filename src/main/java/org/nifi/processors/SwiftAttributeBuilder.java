package org.nifi.processors;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.javaswift.joss.model.StoredObject;

import java.util.HashMap;
import java.util.Map;

public class SwiftAttributeBuilder {

    public static Map<String, String> fromStoredObject(String containerName, String objectName, StoredObject obj) {
        Map<String, String> attrs = new HashMap<>();
        attrs.put("swift.container", containerName);
        attrs.put("swift.filename", objectName);

        if (obj.getEtag() != null) {
            attrs.put("swift.etag", obj.getEtag());
        }
        attrs.put("swift.length", String.valueOf(obj.getContentLength()));
        if (obj.getLastModifiedAsDate() != null) {
            attrs.put("swift.lastModified", String.valueOf(obj.getLastModifiedAsDate().getTime()));
        }
        if (obj.getContentType() != null && !obj.getContentType().isEmpty()) {
            attrs.put(CoreAttributes.MIME_TYPE.key(), obj.getContentType());
        }
        return attrs;
    }
}
