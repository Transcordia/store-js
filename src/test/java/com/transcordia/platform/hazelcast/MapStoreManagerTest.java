package com.transcordia.platform.hazelcast;

import com.hazelcast.core.IMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.*;

@ContextConfiguration(locations = {"classpath:spring-context.xml"})
public class MapStoreManagerTest extends AbstractTestNGSpringContextTests {

    // Constants -------------------------------------------------------------------

    private static final String MAPNAME = "test-mapstore";

    // Instances -------------------------------------------------------------------

    @Autowired(required = true)
    private HazelcastBootstrap _hazel;

    private IMap<Object, Object> _mapHazel;

    // Test setup and teardown -----------------------------------------------------

    @BeforeClass
    public void setUp() throws InterruptedException {
        _mapHazel = _hazel.getHazelcast().getMap(MAPNAME);
    }

    @AfterClass
    public void tearDown() {
    }

    @BeforeMethod
    public void beforeMethod() throws InterruptedException {
    }

    // Tests -----------------------------------------------------------------------

    @Test
    public void testHazelcastStore() throws Exception {
        final Map<Object, Object> localMap = new HashMap<Object, Object>();

        // Writing 1000 JSON strings to the map
        for (int i = 0; i < 1000; i++) {
            String key = "key" + i;
            String value = String.format("{\"%s\":\"%.0f\"}", key, Math.random() * 10000);
            _mapHazel.put(key, value);
            localMap.put(key, value);
        }

        // Inspect the map used to persist the data to make sure all are accounted for
        for (int i = 0; i < 1000; i++) {
            String key = "key" + i;
            final Object value = _mapHazel.get(key);
            assertEquals(value, localMap.get(key));
        }

        String key = "key" + 1000;
        final Object value = _mapHazel.get(key);
        assertNull(value);
    }

    @Test
    public void testHazelcastDelete() throws Exception {
        // Writing 1000 JSON strings to the map
        for (int i = 0; i < 1000; i++) {
            String key = "key" + i;
            String value = String.format("{\"%s\":\"%.0f\"}", key, Math.random() * 10000);
            _mapHazel.put(key, value);
        }

        // Remove all of the even keys
        for (int i = 0; i < 1000; i++) {
            String key = "key" + i;
            if (i % 2 == 0) _mapHazel.remove(key);
        }


        // Inspect the map to ensure odd keys are there and even keys are gone
        for (int i = 0; i < 1000; i++) {
            String key = "key" + i;
            final Object value = _mapHazel.get(key);
            if (i % 2 == 0) assertNull(value);
            else assertNotNull(value);
        }
    }
}
