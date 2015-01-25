package com.transcordia.platform.hazelcast.persistence;

import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStoreFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.Properties;

/**
 * The delegating map store adds the ability to tie one or more MapStore implementations to the
 * Hazelcast persistence mechanism. The map stores are configured in Hazelcast.xml even though
 * they are bean prototypes defined in Spring.
 *
 * <p/>
 *
 */
public class DelegatingMapStoreFactory implements MapStoreFactory, ApplicationContextAware {

    // Constants -------------------------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(DelegatingMapStoreFactory.class);

    // Instances -------------------------------------------------------------------

    protected ApplicationContext _applicationContext;

    // Constructors ----------------------------------------------------------------

    public DelegatingMapStoreFactory() {
        super();
        LOG.info("Instantiating DelegatingMapStoreFactory: {}", this);
    }

    // ApplicationContextAware implementation --------------------------------------

    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        LOG.debug("Setting application context: {}", applicationContext);
        _applicationContext = applicationContext;
    }


    // MapStoreFactory implementation ----------------------------------------------

    public MapLoader newMapStore(String mapName, Properties properties) {
        LOG.info("Creating a newMapStore using factory {}, ApplicationContext: {}",
                this, _applicationContext);

        // Create a new MapStore implementation that delegates to the Spring beans with these
        // names. Fetching the Spring bean will create a new instance of that MapStore.
        DelegatingMapStore delegatingMapStore = new DelegatingMapStore();

        // Determine what the map loader is for this mapName and load the bean
        String maploader = properties.getProperty("maploader");
        if (maploader == null) throw new IllegalArgumentException(
                "In the Hazelcast configuration, map store must have a 'maploader' " +
                        "property specified."
        );

        // Read the mapstore property to discover which mapstore implementations will be used to
        // store this particular mapname.
        String mapstore = properties.getProperty("mapstore");
        if (mapstore == null) throw new IllegalArgumentException(
                "In the Hazelcast configuration, map store must have a 'mapstore' " +
                        "property specified."
        );

        // Multiple stores can be supplied using a comma or whitespace to separate.
        String[] storeIds = mapstore.split("[,\\s]+");
        LOG.debug("For mapName {}, found stores: {}", mapName, storeIds);

        // If the MapLoader is also in the MapStore list, we will discover it and reuse the same
        // map instead of creating a separate instance for the loader and the store.
        MapPersistence mapLoader = null;

        for (String storeId : storeIds) {
            MapPersistence mapPersistence = (MapPersistence)_applicationContext.getBean(storeId);
            delegatingMapStore.addMapPersistence(mapPersistence);
            if (storeId.equals(maploader)) mapLoader = mapPersistence;
        }

        // If we don't yet have a mapLoader, we need to fetch it from Spring.
        if (mapLoader == null) mapLoader = (MapPersistence)_applicationContext.getBean(maploader);

        // Set the loader function for the map store.
        delegatingMapStore.setMapLoader(mapLoader);


        return delegatingMapStore;
    }
}
