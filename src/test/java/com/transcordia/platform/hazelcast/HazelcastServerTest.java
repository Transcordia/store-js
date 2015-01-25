package com.transcordia.platform.hazelcast;

import com.hazelcast.config.TcpIpConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@ContextConfiguration(locations = {"classpath:spring-context.xml"})
public class HazelcastServerTest extends AbstractTestNGSpringContextTests {

    @Autowired(required = true)
    private HazelcastBootstrap _hz;

    // Test setup and teardown -----------------------------------------------------

    @BeforeClass
    public void setUp() throws InterruptedException {
    }

    @AfterClass
    public void tearDown() {
    }

    @BeforeMethod
    public void beforeMethod() throws InterruptedException {
    }

    // Tests -----------------------------------------------------------------------

    @Test
    public void testServerStarted() throws Exception {
        assertNotNull(_hz);

        final TcpIpConfig config = _hz.getHazelcast().getConfig()
                .getNetworkConfig().getJoin().getTcpIpConfig();

        final List<String> members = config.getMembers();
        assertTrue(members.size() > 0);
    }

}
