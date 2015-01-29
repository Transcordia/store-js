package com.transcordia.platform.hazelcast.persistence.elasticsearch;

import com.hazelcast.core.HazelcastInstance;
import com.transcordia.platform.hazelcast.persistence.MapPersistence;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

public class ElasticSearchMapPersistence implements MapPersistence {

    // Constants -------------------------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchMapPersistence.class);
    private static final String NOT_FOUND = "No key found in Elastic Search when loading index: %s, type: %s, key: %s";
    private static final String ERR_INDEX_CREATION = "Failed to create index %s.";

    // Instances -------------------------------------------------------------------

    protected String _index;
    protected String _type;
    protected Client _esClient;
    protected Charset _encoding;


    // Constructors ----------------------------------------------------------------

    public ElasticSearchMapPersistence() {
        LOG.info("Initializing ElasticSearchMapPersistence instance.");
        try {
            _encoding = Charset.forName("UTF-8");
        } catch (Exception e) {
            throw new RuntimeException("Charset UTF-8 not supported.");
        }
    }


    // Protecteds ------------------------------------------------------------------

    protected String[] mapParts(String mapName) {
        String[] parts = mapName.split("-");
        if (parts.length != 2) {
            throw new IllegalArgumentException("The Hazelcast map name must be in the format <index>-<type>.");
        }

        return parts;
    }

    protected void checkForIndex(String esIndex) {
        final ClusterAdminClient cluster = _esClient.admin().cluster();
        cluster.prepareHealth().setWaitForYellowStatus().execute().actionGet();

        ClusterStateResponse response = cluster.prepareState().execute().actionGet();
        boolean hasIndex = response.getState().metaData().hasIndex(esIndex);

        if (!hasIndex) {
            LOG.info("Index Not Found, creating index: " + esIndex);

            _esClient.admin().indices().prepareCreate(esIndex).execute().actionGet();
            LOG.info("Created index, waiting for Yellow state: " + esIndex);

            final ClusterHealthResponse health = cluster.prepareHealth()
                    .setWaitForYellowStatus()
                    .execute().actionGet();

            response = cluster.prepareState().execute().actionGet();
            hasIndex = response.getState().metaData().hasIndex(esIndex);

            if (!hasIndex)
                throw new RuntimeException(String.format(ERR_INDEX_CREATION, esIndex));
            LOG.info("Index {} created and cluster is in {} state.", esIndex, health.getStatus().name());
        }
        LOG.info("Index {} already exists.", esIndex);
    }


    // Properties ------------------------------------------------------------------

    public void setEsClient(Client esClient) {
        _esClient = esClient;
    }


    // MapLoaderLifecycleSupport ---------------------------------------------------

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
        if (LOG.isInfoEnabled()) {
            LOG.info("Initializing the ES persistence for map {}. Additional props: {}",
                    mapName, properties.toString());
        }
        final String[] parts = mapParts(mapName);
        _index = parts[0];
        _type = parts[1];
        checkForIndex(_index);
    }

    /**
     * Hazelcast will call this method before shutting down.
     * This method can be overridden to cleanup the resources
     * held by this map loader implementation, such as closing the
     * database connections etc.
     */
    public void destroy() {
        _esClient.close();
    }


    // MapStore Implementation -----------------------------------------------------


    /**
     * Deletes the entry with a given key from the store.
     *
     * @param key key to delete from the store.
     */
    public void delete(Object key) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Deleting document, index [{}], type [{}], key [{}]",
                    new Object[]{_index, _type, key});
        }

        final DeleteRequest request = _esClient.prepareDelete(_index, _type, (String) key).request();

        try {
            _esClient.delete(request).actionGet();
        } catch (ElasticsearchException e) {
            LOG.warn("Failed to delete, index [{}], type [[]], key [{}]",
                    new Object[]{_index, _type, key}, e);
        }
    }

    /**
     * Deletes multiple entries from the store.
     * todo: Replace this with the ES Bulk API call.
     *
     * @param keys keys of the entries to delete.
     */
    public void deleteAll(Collection keys) {
        for (Object key : keys) {
            delete(key);
        }
    }

    /**
     * Stores multiple entries. Implementation of this method can optimize the
     * store operation by storing all entries in one database connection for instance.
     * <p/>
     * todo: Replace this with the ES Bulk API call.
     *
     * @param map map of entries to store
     */
    public void storeAll(Map map) {
        for (Object item : map.entrySet()) {
            Map.Entry entry = (Map.Entry) item;
            store(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Stores the key-value pair.
     *
     * @param key   key of the entry to store
     * @param value value of the entry to store
     */
    public void store(Object key, Object value) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("store, index: %s, mapName: %s, key: %s, instance: %s",
                    _index, _type, key, this.toString()));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Indexing document, index [{}], type [{}], key [{}]",
                    new Object[]{_index, _type, key});
        }

        final IndexRequestBuilder builder = _esClient.prepareIndex(_index, _type, (String) key);

        if (value instanceof String) {
            builder.setSource((String) value);
        } else if (value instanceof byte[]) {
            builder.setSource((byte[]) value);
        } else
            throw new IllegalArgumentException("This persistor can only save byte[] and String types.");

        try {
            _esClient.index(builder.request()).actionGet();
        } catch (ElasticsearchException e) {
            LOG.warn(String.format("Failed to index, index [%s], type [%s], key [%s], value [%s]",
                    _index, _type, key, value), e);
            throw e;
        }
    }

    // MapLoader Implementation ----------------------------------------------------


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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Loading document, index [{}], type [{}], key [{}]",
                    new Object[]{_index, _type, key});
        }

        final GetResponse response = _esClient.prepareGet(_index, _type, (String) key)
                .setRealtime(true)
                .execute()
                .actionGet();

        if (response.isSourceEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format(NOT_FOUND, response.getIndex(), response.getType(), response.getId()));
            }
            return null;
        }

        // In case of a single result, our source can be retrieved
        Map<String, Object> result = response.getSource();
        result.put("_type", response.getType());
        result.put("_index", response.getIndex());
        result.put("_version", response.getVersion());

        try {
            return XContentFactory.jsonBuilder().map(result).string();
        } catch (IOException e) {
            LOG.error("Failed to convert map to JSON string loading key [" + key + "]", e);
        }

        return null;
    }

    /**
     * Loads given keys. This is batch load operation so that implementation can
     * optimize the multiple loads.
     *
     * @param keys keys of the values entries to load
     * @return map of loaded key-value pairs.
     */
    public Map loadAll(Collection keys) {
        Map result = new HashMap(keys.size());
        for (Object key : keys) {
            result.put(key, load(key));
        }
        return result;
    }

    /**
     * Loads all of the keys from the store.
     *
     * @return all the keys
     */
    public Set loadAllKeys() {
        return null;
    }
}
