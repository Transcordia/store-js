package com.transcordia.platform.hazelcast.persistence;

import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class InMemoryMapPersistence implements MapPersistence {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryMapPersistence.class);
    protected Map<Object, Object> _store = new HashMap<Object, Object>();

    /**
     * Deletes the entry with a given key from the store.
     *
     * @param key     key to delete from the store.
     */
    public void delete(Object key) {
        _store.remove(key);
    }

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

    }

    /**
     * Hazelcast will call this method before shutting down.
     */
    public void destroy() {
    }

    /**
     * Loads the value of a given key.
     *
     * @param key     The key of the object to load.
     * @return value of the key
     */
    public Object load(Object key) {
        LOG.debug("Loading key: {}, from map: {}", key, this);
        return _store.get(key);
    }

    /**
     * Loads given keys. This is batch load operation so that implementation can optimize the
     * multiple loads.
     *
     * @param keys    keys of the values entries to load
     * @return map of loaded key-value pairs.
     */
    public Map loadAll(Collection keys) {
        Map<Object, Object> result = new HashMap<Object, Object>();
        for (Object key : keys) {
           result.put(key, _store.get(key));
        }

        return result;
    }

    /**
     * Loads all of the keys from the store.
     *
     * @return all the keys
     */
    public Set loadAllKeys() {
        return _store.keySet();
    }

    /**
     * Stores the key-value pair.
     *
     * @param key     key of the entry to store
     * @param value   value of the entry to store
     */
    public void store(Object key, Object value) {
        LOG.debug("Putting key: {} and value: {} into map: {}", new Object[] {key, value, this});
        _store.put(key, value);
    }

    /**
     * Stores multiple entries. Implementation of this method can optimize the store operation by
     * storing all entries in one database connection for instance.
     *
     * @param all     map of entries to store
     */
    public void storeAll(Map all) {
        for (Object item : all.entrySet()) {
            Map.Entry<Object, Object> entry = (Map.Entry <Object,Object>) item;
            _store.put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Deletes multiple entries from the store.
     *
     * @param keys    keys of the entries to delete.
     */
    public void deleteAll(Collection keys) {
        for (Object key : keys) {
            _store.remove(key);
        }
    }

    // For testing
    public void deleteAll() {
        _store.clear();
    }
}
