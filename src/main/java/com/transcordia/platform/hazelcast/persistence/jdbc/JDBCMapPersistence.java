package com.transcordia.platform.hazelcast.persistence.jdbc;

import com.hazelcast.core.HazelcastInstance;
import com.transcordia.platform.hazelcast.persistence.MapPersistence;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class JDBCMapPersistence implements MapPersistence {
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
//import java.sql.Connection;
//import java.sql.DatabaseMetaData;
//import java.sql.ResultSet;
//import java.sql.SQLException;
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
//import javax.sql.DataSource;
//
//import com.amazonaws.AmazonClientException;
//import com.amazonaws.auth.AWSCredentials;
//import com.amazonaws.auth.BasicAWSCredentials;
//import com.amazonaws.services.simpledb.AmazonSimpleDBAsync;
//import com.amazonaws.services.simpledb.AmazonSimpleDBAsyncClient;
//import com.amazonaws.services.simpledb.model.Attribute;
//import com.amazonaws.services.simpledb.model.CreateDomainRequest;
//import com.amazonaws.services.simpledb.model.DeleteAttributesRequest;
//import com.amazonaws.services.simpledb.model.GetAttributesRequest;
//import com.amazonaws.services.simpledb.model.GetAttributesResult;
//import com.amazonaws.services.simpledb.model.PutAttributesRequest;
//import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
//import com.transcordia.platform.hazelcast.persistence.MapPersistence;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.dao.DataAccessException;
//import org.springframework.jdbc.core.JdbcTemplate;
//
//
///**
// * The Hazelcast API stores and loads values from a store by providing a map name and a key. The map
// * name is a composite of <index>-<type>, so we actually have three components; index, type, and
// * key.
// * <p/>
// * In JDBC terminology, these Hazelcast components map thusly:
// * <p/>
// * Hazelcast  maps to   JDBC
// * --------------------------------
// * Map Name             Table (map name is in the format <index>-<type>
// * Key                  Primary Key
// * Value                Value (Text)
// */
//public class JDBCMapPersistence implements MapPersistence {
//
//    // Constants -------------------------------------------------------------------
//
//    private static final Logger LOG = LoggerFactory.getLogger(JDBCMapPersistence.class);
//
//    private static final String NOT_FOUND = "No key found when loading index: %s, type: %s, key: %s";
//    private static final String ERR_INDEX_CREATION = "Failed to create index %s.";
//
//    // Instances -------------------------------------------------------------------
//
//    protected JdbcTemplate _jdbcTemplate;
//
//    // Constructors ----------------------------------------------------------------
//
//    public JDBCMapPersistence() {
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
//    protected void checkForDomain(String tableName) {
//        if (LOG.isDebugEnabled()) {
//            LOG.debug(String.format("Checking for existence of tableName [%s]", tableName));
//        }
//
//        Connection conn = null;
//        try {
//            conn = _jdbcTemplate.getDataSource().getConnection();
//
//            final DatabaseMetaData md = conn.getMetaData();
//            final ResultSet tables = md.getTables(null, null, null, new String[]{"TABLE"});
//
//            while (tables.next()) {
//                if (tableName.equalsIgnoreCase(tables.getString("TABLE_NAME"))) {
//                    if (LOG.isDebugEnabled()) {
//                        LOG.debug(String.format("Table [%s] exists.", tableName));
//                    }
//                    return;
//                }
//            }
//
//        } catch (SQLException e) {
//            LOG.warn(String.format("Error while checking for existence of table [%s].", tableName), e);
//        } finally {
//            try {
//                if (conn != null) conn.close();
//            } catch (SQLException e) {
//                LOG.warn("Error closing database connection.", e);
//            }
//        }
//
//
//        if (LOG.isDebugEnabled()) {
//            LOG.debug(String.format("Table [%s] does not exist; creating.", tableName));
//        }
//
//        String sql = String.format(
//                "CREATE TABLE %s (KEY_ID VARCHAR(255), VALUE TEXT)"
//                , tableName);
//
//        try {
//            _jdbcTemplate.execute(sql);
//        } catch (DataAccessException e) {
//            throw new RuntimeException(
//                    String.format("Failed to create JDBC table [%s].", tableName));
//        }
//
//        if (LOG.isInfoEnabled()) {
//            LOG.info(String.format("Index [%s] created as a JDBC table.", tableName));
//        }
//    }
//
//    // Properties ------------------------------------------------------------------
//
//    public void setDataSource(DataSource dataSource) {
//        _jdbcTemplate = new JdbcTemplate(dataSource);
//    }
//
//// MapPersistence Implementation -----------------------------------------------
//
//    /**
//     * Initializes this MapStore implementation.
//     *
//     * @param properties Properties set for this mapStore. see MapStoreConfig
//     * @param mapName    name of the map.
//     */
//    public void init(Properties properties, String mapName) {
//        if (LOG.isInfoEnabled()) {
//            LOG.info("Initializing persistence for map {}. Additional props: {}",
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
//            _jdbcTemplate.
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
