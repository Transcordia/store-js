package com.transcordia.platform.hazelcast.persistence.simpledb;

import com.hazelcast.core.HazelcastInstance;
import com.transcordia.platform.hazelcast.persistence.MapPersistence;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class SimpleDBMapPersistence implements MapPersistence {
    /**
     * Initializes this MapLoader implementation. Hazelcast will call
     * this method when the map is first used on the
     * HazelcastInstance. Implementation can
     * initialize required resources for the implementing
     * mapLoader such as reading a config file and/or creating
     * database connection.
     *
     * @param hazelcastInstance HazelcastInstance of this mapLoader.
     * @param properties        Properties set for this mapStore. see MapStoreConfig
     * @param mapName           name of the map.
     */
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Hazelcast will call this method before shutting down.
     * This method can be overridden to cleanup the resources
     * held by this map loader implementation, such as closing the
     * database connections etc.
     */
    public void destroy() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Stores the key-value pair.
     *
     * @param key   key of the entry to store
     * @param value value of the entry to store
     */
    public void store(Object key, Object value) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Stores multiple entries. Implementation of this method can optimize the
     * store operation by storing all entries in one database connection for instance.
     *
     * @param map map of entries to store
     */
    public void storeAll(Map map) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Deletes the entry with a given key from the store.
     *
     * @param key key to delete from the store.
     */
    public void delete(Object key) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Deletes multiple entries from the store.
     *
     * @param keys keys of the entries to delete.
     */
    public void deleteAll(Collection keys) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Loads the value of a given key. If distributed map doesn't contain the value
     * for the given key then Hazelcast will call implementation's load (key) method
     * to obtain the value. Implementation can use any means of loading the given key;
     * such as an O/R mapping tool, simple SQL or reading a file etc.
     *
     * @param key
     * @return value of the key
     */
    public Object load(Object key) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Loads given keys. This is batch load operation so that implementation can
     * optimize the multiple loads.
     *
     * @param keys keys of the values entries to load
     * @return map of loaded key-value pairs.
     */
    public Map loadAll(Collection keys) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Loads all of the keys from the store.
     *
     * @return all the keys
     */
    public Set loadAllKeys() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
//
//import java.nio.charset.Charset;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.Collections;
//import java.util.Comparator;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;
//import java.util.Set;
//
//import com.amazonaws.AmazonClientException;
//import com.amazonaws.auth.AWSCredentials;
//import com.amazonaws.auth.BasicAWSCredentials;
//import com.amazonaws.services.simpledb.AmazonSimpleDBAsync;
//import com.amazonaws.services.simpledb.AmazonSimpleDBAsyncClient;
//import com.amazonaws.services.simpledb.model.Attribute;
//import com.amazonaws.services.simpledb.model.CreateDomainRequest;
//import com.amazonaws.services.simpledb.model.DeleteAttributesRequest;
//import com.amazonaws.services.simpledb.model.DomainMetadataRequest;
//import com.amazonaws.services.simpledb.model.GetAttributesRequest;
//import com.amazonaws.services.simpledb.model.GetAttributesResult;
//import com.amazonaws.services.simpledb.model.NoSuchDomainException;
//import com.amazonaws.services.simpledb.model.PutAttributesRequest;
//import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
//import com.amazonaws.util.HttpUtils;
//import com.transcordia.platform.hazelcast.persistence.MapPersistence;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//
///**
// * The Hazelcast API stores and loads values from a store by providing a map name and a key. The map
// * name is a composite of <index>-<type>, so we actually have three components; index, type, and
// * key.
// * <p/>
// * In SimpleDB terminology, these Hazelcast components map thusly:
// * Account = Determined by AWS credentials
// * Domain = Index-Type (we use the composite value)
// * Item = Key (must be unique per record)
// * Attribute = "KEY-XXXX", an attribute value cannot exceed 1K, so we chunk it up into consecutively
// * numbered strings beginning with KEY-0000 and going to KEY-nnnn
// * Value = String representation of JSON object chunked up across keys.
// */
//public class SimpleDBMapPersistence implements MapPersistence {
//
//    // Constants -------------------------------------------------------------------
//
//    private static final Logger LOG = LoggerFactory.getLogger(SimpleDBMapPersistence.class);
//
//    private static final String NOT_FOUND = "No key found in SimpleDB when loading index: %s, type: %s, key: %s";
//    private static final String ERR_INDEX_CREATION = "Failed to create index %s.";
//    protected static final int CHUNK_SIZE = 1024;
//
//    // Instances -------------------------------------------------------------------
//
//    protected Charset _encoding;
//    protected String _awsAccessKey;
//    protected String _awsSecretKey;
//    protected String _endpoint;
//    protected AmazonSimpleDBAsync _client;
//
//    // Constructors ----------------------------------------------------------------
//
//    public SimpleDBMapPersistence() {
//        try {
//            _encoding = Charset.forName("UTF-8");
//        } catch (Exception e) {
//            throw new RuntimeException("Charset UTF-8 not supported.");
//        }
//    }
//
//
//    // Protecteds ------------------------------------------------------------------
//
//    protected String[] mapParts(String mapName) {
//        String[] parts = mapName.split("-");
//        if (parts.length != 2) {
//            throw new IllegalArgumentException("The Hazelcast map name must be in the format <index>-<type>.");
//        }
//
//        return parts;
//    }
//
//    protected void checkForDomain(String domain) {
//        if (LOG.isDebugEnabled()) {
//            LOG.debug(String.format("Checking for existence of index [%s]", domain));
//        }
//        boolean domainExists = true;
//        try {
//            final DomainMetadataRequest metadataRequest = new DomainMetadataRequest(domain);
//            getClient().domainMetadata(metadataRequest);
//        } catch (NoSuchDomainException e) {
//            domainExists = false;
//        }
//
//        if (!domainExists) {
//            if (LOG.isDebugEnabled()) {
//                LOG.debug(String.format("Index [%s] does not exist; creating.", domain));
//            }
//
//            final CreateDomainRequest createDomainRequest = new CreateDomainRequest(domain);
//
//            try {
//                getClient().createDomain(createDomainRequest);
//            } catch (AmazonClientException e) {
//                throw new RuntimeException(
//                        String.format("Failed to create SimpleDB domain [%s].", domain));
//            }
//
//            if (LOG.isInfoEnabled()) {
//                LOG.info(String.format("Index [%s] created as a SimpleDB domain.", domain));
//            }
//        } else {
//            if (LOG.isDebugEnabled()) {
//                LOG.debug(String.format("Index [%s] exists.", domain));
//            }
//        }
//
//
//    }
//
//
//    protected synchronized AmazonSimpleDBAsync getClient() {
//        if (_client == null) {
//            final AWSCredentials credentials = new BasicAWSCredentials(_awsAccessKey, _awsSecretKey);
//            if (LOG.isInfoEnabled()) {
//                LOG.info("Creating Amazon Async Client using credentials ({}, {})", _awsAccessKey, _awsSecretKey);
//            }
//            _client = new AmazonSimpleDBAsyncClient(credentials);
////            _client = new AmazonSimpleDBClient(credentials);
//            if (_endpoint != null) _client.setEndpoint(_endpoint);
//        }
//        return _client;
//    }
//
//    // Properties ------------------------------------------------------------------
//
//    public void setEncoding(String encoding) {
//        try {
//            _encoding = Charset.forName(encoding);
//        } catch (Exception e) {
//            throw new RuntimeException("Charset " + encoding + " not supported.");
//        }
//    }
//
//    public void setAwsAccessKey(String awsAccessKey) {
//        _awsAccessKey = awsAccessKey;
//    }
//
//    public void setAwsSecretKey(String awsSecretKey) {
//        _awsSecretKey = awsSecretKey;
//    }
//
//    public void setEndpoint(String endpoint) {
//        _endpoint = endpoint;
//    }
//
//    // MapPersistence Implementation -----------------------------------------------
//
//    /**
//     * Initializes this MapStore implementation.
//     *
//     * @param properties Properties set for this mapStore. see MapStoreConfig
//     * @param mapName    name of the map.
//     */
//    public void init(Properties properties, String mapName) {
//        if (LOG.isInfoEnabled()) {
//            LOG.info("Initializing SimpleDB persistence for map {}. Additional props: {}",
//                    mapName, properties.toString());
//        }
//        checkForDomain(mapName);
//    }
//
//
//    /**
//     * Deletes the entry with a given key from the store.
//     *
//     * @param mapName name of the map.
//     * @param key     key to delete from the store.
//     */
//    public void delete(String mapName, final Object key) {
//        final String[] parts = mapParts(mapName);
//
//        try {
//            if (LOG.isDebugEnabled()) {
//                LOG.debug("Deleting document, index [{}], type [{}], key [{}]",
//                        new Object[]{parts[0], parts[1], key});
//            }
//
//            final DeleteAttributesRequest deleteAttributesRequest =
//                    new DeleteAttributesRequest(mapName, (String) key);
//
//            getClient().deleteAttributes(deleteAttributesRequest);
//        } catch (Throwable e) {
//            LOG.error(String.format("Failure while deleting document, index [%s], type [%s], key [%s]",
//                    parts[0], parts[1], key), e);
//        }
//
//    }
//
//    /**
//     * Hazelcast will call this method before shutting down.
//     *
//     * @param mapName name of the map.
//     */
//    public void destroy(String mapName) {
//        if (LOG.isDebugEnabled()) {
//            final String[] parts = mapParts(mapName);
//
//            LOG.debug("Shutting down Hazelcast -> SimpleDB persistence for index [{}] and type [{}]",
//                    parts[0], parts[1]);
//        }
//    }
//
//    /**
//     * Loads the value of a given key.
//     *
//     * @param mapName name of the map.
//     * @param key     The key of the object to load.
//     * @return value of the key
//     */
//    public Object load(String mapName, Object key) {
//        final String[] parts = mapParts(mapName);
//
//        if (LOG.isDebugEnabled()) {
//            LOG.debug("Loading document, index [{}], type [{}], key [{}]",
//                    new Object[]{parts[0], parts[1], key});
//        }
//
//        final GetAttributesResult attributesResult =
//                getClient().getAttributes(new GetAttributesRequest(mapName, (String) key));
//
//        List<Attribute> attrs = attributesResult.getAttributes();
//
//        if (attrs.size() == 0) return null;
//
//        // Sort the attributes by name
//        Collections.sort(attrs, new Comparator<Attribute>() {
//            public int compare(Attribute o1, Attribute o2) {
//                return o1.getName().compareTo(o2.getName());
//            }
//        });
//
//        StringBuilder sb = new StringBuilder(attrs.size() * CHUNK_SIZE);
//        for (Attribute attr : attrs) {
//            sb.append(attr.getValue());
//        }
//
//        return sb.toString();
//    }
//
//    /**
//     * Loads given keys. This is batch load operation so that implementation can optimize the
//     * multiple loads.
//     *
//     * @param mapName name of the map.
//     * @param keys    keys of the values entries to load
//     * @return map of loaded key-value pairs.
//     */
//    @SuppressWarnings({"unchecked"})
//    public Map loadAll(String mapName, Collection keys) {
//        Map result = new HashMap(keys.size());
//        for (Object key : keys) {
//            result.put(key, load(mapName, key));
//        }
//        return result;
//    }
//
//    /**
//     * Loads all of the keys from the store.
//     *
//     * @param mapName name of the map.
//     * @return all the keys
//     */
//    public Set loadAllKeys(String mapName) {
//        return null;
//    }
//
//    /**
//     * Stores the key-value pair.
//     *
//     * @param mapName name of the map.
//     * @param key     key of the entry to store
//     * @param value   value of the entry to store
//     */
//    public void store(String mapName, final Object key, Object value) {
//        final String[] parts = mapParts(mapName);
//        try {
//
////            String sVal = HttpUtils.urlEncode((String) value, false);
//            String sVal = (String) value;
//
//            if (sVal.length() > 256 * 1024) {
//                String msg = String.format(
//                        "Document size [%,d] is too large, index [%s], type [%s], key [%s]. Maximum payload is %,d bytes.",
//                        sVal.length(), parts[0], parts[1], key, 256 * 1024);
//                throw new RuntimeException(msg);
//            }
//            if (LOG.isDebugEnabled()) {
//                LOG.debug("Indexing document, index [{}], type [{}], key [{}]",
//                        new Object[]{parts[0], parts[1], key});
//            }
//
//            // Because we use the SimpleDB attributes as a way to chunk up the stored value, we have
//            // a situation where we may end up with fewer attributes than may have been previously
//            // stored. For this reason, we delete any existing attributes before we put the new attrs.
//            final DeleteAttributesRequest deleteAttributesRequest =
//                    new DeleteAttributesRequest(mapName, (String) key);
//            getClient().deleteAttributes(deleteAttributesRequest);
//
//
//            // Create a list of name/value pairs to represent the new value chunks.
//            final ArrayList<ReplaceableAttribute> attrs = new ArrayList<ReplaceableAttribute>();
//
//            // The value of an attribute in SimpleDB can be a maximum of 1024 bytes. We need to chunk up
//            // our value.
//            final char[] chars = sVal.toCharArray();
//            for (int i = 0, j = 0, c = chars.length; i < c; i += CHUNK_SIZE, j++) {
//                String s = new String(Arrays.copyOfRange(chars, i, Math.min(i + CHUNK_SIZE, c)));
//                String k = String.format("KEY-%04d", j);
//                attrs.add(new ReplaceableAttribute(k, s, false));
//            }
//
//            LOG.debug("Writing attributes: " + attrs);
//
//            // Store the new chunked values
//            getClient().putAttributes(new PutAttributesRequest(
//                    mapName, (String) key, attrs
//            ));
//        } catch (RuntimeException e) {
//            LOG.error(String.format("Failure while indexing document, index [%s], type [%s], key [%s]",
//                    parts[0], parts[1], key), e);
//        }
//    }
//
//    /**
//     * Stores multiple entries.
//     * todo: Replace this with the ES Bulk API call.
//     *
//     * @param mapName name of the map.
//     * @param map     map of entries to store
//     */
//    public void storeAll(String mapName, Map map) {
//        final String[] parts = mapParts(mapName);
//        if (LOG.isDebugEnabled()) {
//            LOG.debug("Indexing multiple documents, index [{}], type [{}], item count [{}]",
//                    new Object[]{parts[0], parts[1], map.size()});
//        }
//
//        for (Object item : map.entrySet()) {
//            Map.Entry entry = (Map.Entry) item;
//            store(mapName, entry.getKey(), entry.getValue());
//        }
//    }
//
//    /**
//     * Deletes multiple entries from the store.
//     * todo: Replace this with the ES Bulk API call.
//     *
//     * @param mapName name of the map.
//     * @param keys    keys of the entries to delete.
//     */
//    public void deleteAll(String mapName, Collection keys) {
//        final String[] parts = mapParts(mapName);
//
//        if (LOG.isDebugEnabled()) {
//            LOG.debug("Deleting multiple documents, index [{}], type [{}], item count [{}]",
//                    new Object[]{parts[0], parts[1], keys.size()});
//        }
//
//        for (Object key : keys) {
//            delete(mapName, key);
//        }
//    }
//}
