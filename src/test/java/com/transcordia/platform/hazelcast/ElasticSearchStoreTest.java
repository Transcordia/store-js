package com.transcordia.platform.hazelcast;

import com.hazelcast.core.IMap;
import com.transcordia.platform.elasticsearch.ElasticSearchServer;
import org.elasticsearch.client.Client;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.*;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.*;

@ContextConfiguration(locations = {"classpath:spring-context.xml"})
public class ElasticSearchStoreTest extends AbstractTestNGSpringContextTests {

    // Constants -------------------------------------------------------------------

    private static final String MAPNAME = "test-elasticsearch";

    // Instances -------------------------------------------------------------------

    @Autowired(required = true)
    private HazelcastBootstrap _hazel;

    @Autowired(required = true)
    private ElasticSearchServer _esServer;

    private IMap<Object, Object> _mapHazel;


    /**
     * Adding this to make sure the injection happens before the setUp() function runs.
     *
     * @throws Exception
     */
    @BeforeSuite(alwaysRun = true)
    @BeforeClass(alwaysRun = true)
    @Override
    protected void springTestContextPrepareTestInstance() throws Exception {
        super.springTestContextPrepareTestInstance();
    }


    // Test setup and teardown -----------------------------------------------------

    @BeforeClass
    public void setUp() throws InterruptedException {
        assertNotNull(_hazel);
        _mapHazel = _hazel.getHazelcast().getMap(MAPNAME);
    }

    @AfterClass
    public void tearDown() {
        // Things shutdown cleaner when there aren't any pending updates/deletes to ES.
        esRefresh();
    }

    @BeforeMethod
    public void beforeMethod() throws InterruptedException {
        _mapHazel.clear();
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
            // Can't test loads from es if key is in hazelcast
            _mapHazel.evict(key);
        }

        // Force the loading of each key from Elastic Search
        for (int i = 0; i < 1000; i++) {
            String key = "key" + i;
            final String value = (String) _mapHazel.get(key);
            assertNotNull(value);
            // Value from hazelcast has been loaded from ES, so it will have additional props
            // like _version and _type. Local value will be something like {"key0":"5432"}, so
            // just need to strip curlies and look for string in value.
            String localValue = (String) localMap.get(key);
            localValue = localValue.substring(1, localValue.length() - 1);
            assertTrue(value.contains(localValue));
        }
    }

    @Test(enabled = true, invocationCount = 1)
    public void testHazelcastDelete() throws Exception {
        // Writing 1000 JSON strings to the map
        for (int i = 0; i < 1000; i++) {
            String key = "key" + i;
            String value = String.format("{\"%s\":\"%.0f\"}", key, Math.random() * 10000);
            _mapHazel.put(key, value);
        }

        // Elastic Search is asynchronous. Let it catch up
        esRefresh();

        // Remove all of the even keys, evict all the odd keys
        for (int i = 0; i < 1000; i++) {
            String key = "key" + i;
            if (i % 2 == 0) _mapHazel.remove(key);
            else _mapHazel.evict(key);
        }

        // Inspect the underlying map to ensure odd keys are there and even keys are gone
        for (int i = 0; i < 1000; i++) {
            String key = "key" + i;
            final Object value = _mapHazel.get(key);
            if (i % 2 == 0) assertNull(value, "Expected null: " + value);
            else assertNotNull(value, "Expected a value: " + value);
        }
    }

    // Protecteds ------------------------------------------------------------------

    protected void esRefresh() {
        final Client client = _esServer.getClient();
        client.admin().indices()
                .prepareRefresh("test")
                .execute().actionGet();
    }
}
