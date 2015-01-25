package com.transcordia.platform.hazelcast.persistence;

import com.hazelcast.core.HazelcastInstance;

import java.util.*;

public class DelegatingMapStore implements MapPersistence {

    // Instances ------------------------------------------------------------------

    List<MapPersistence> _stores = new ArrayList<MapPersistence>();
    MapPersistence _mapLoader;

    // Constructors ----------------------------------------------------------------

    public DelegatingMapStore() {
    }

    // Package Friendly ------------------------------------------------------------

    void addMapPersistence(MapPersistence store) {
        _stores.add(store);
    }

    void setMapLoader(MapPersistence mapLoader) {
        _mapLoader = mapLoader;
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
        return _mapLoader.load(key);
    }

    /**
     * Loads given keys. This is batch load operation so that implementation can
     * optimize the multiple loads.
     *
     * @param keys keys of the values entries to load
     * @return map of loaded key-value pairs.
     */
    public Map loadAll(Collection keys) {
        return _mapLoader.loadAll(keys);
    }

    /**
     * Loads all of the keys from the store.
     *
     * @return all the keys
     */
    public Set loadAllKeys() {
        return _mapLoader.loadAllKeys();
    }


    // MapLoaderLifecycleSupport Implementation ------------------------------------

    /**
     * Initializes this MapLoader implementation. Hazelcast will call
     * this method when the map is first used on the
     * HazelcastInstance. Implementation can
     * initialize required resources for the implementing
     * mapLoader such as reading a config file and/or creating
     * database connection.
     *
     * @param hazelcastInstance HazelcastInstance of this mapLoader.
     * @param properties        Properties set for this mapStore. see MapPersistenceConfig
     * @param mapName           name of the map.
     */
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        _mapLoader.init(hazelcastInstance, properties, mapName);
        for (MapPersistence store : _stores) {
            store.init(hazelcastInstance, properties, mapName);
        }
    }

    /**
     * Hazelcast will call this method before shutting down.
     * This method can be overridden to cleanup the resources
     * held by this map loader implementation, such as closing the
     * database connections etc.
     */
    public void destroy() {
        for (MapPersistence store : _stores) {
            store.destroy();
        }
    }


    // MapPersistence implementation -----------------------------------------------------


    /**
     * Stores the key-value pair.
     *
     * @param key   key of the entry to store
     * @param value value of the entry to store
     */
    public void store(Object key, Object value) {
        for (MapPersistence store : _stores) {
            store.store(key, value);
        }
    }

    /**
     * Stores multiple entries. Implementation of this method can optimize the
     * store operation by storing all entries in one database connection for instance.
     *
     * @param map map of entries to store
     */
    public void storeAll(Map map) {
        for (MapPersistence store : _stores) {
            store.storeAll(map);
        }
    }

    /**
     * Deletes the entry with a given key from the store.
     *
     * @param key key to delete from the store.
     */
    public void delete(Object key) {
        for (MapPersistence store : _stores) {
            store.delete(key);
        }
    }

    /**
     * Deletes multiple entries from the store.
     *
     * @param keys keys of the entries to delete.
     */
    public void deleteAll(Collection keys) {
        for (MapPersistence store : _stores) {
            store.deleteAll(keys);
        }
    }
}
