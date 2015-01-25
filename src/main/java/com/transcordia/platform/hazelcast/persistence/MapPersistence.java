package com.transcordia.platform.hazelcast.persistence;

import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;

public interface MapPersistence extends MapLoader, MapStore, MapLoaderLifecycleSupport {

}
