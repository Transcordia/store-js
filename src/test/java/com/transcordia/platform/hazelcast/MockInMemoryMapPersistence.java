package com.transcordia.platform.hazelcast;

import com.transcordia.platform.hazelcast.persistence.InMemoryMapPersistence;

public class MockInMemoryMapPersistence extends InMemoryMapPersistence {


    public void clear() {
        _store.clear();
    }
}
