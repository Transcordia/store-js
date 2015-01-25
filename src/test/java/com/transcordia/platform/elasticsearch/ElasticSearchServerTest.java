package com.transcordia.platform.elasticsearch;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;

@ContextConfiguration(locations = {"classpath:spring-context.xml"})
public class ElasticSearchServerTest extends AbstractTestNGSpringContextTests {

    @Autowired(required = true)
    private ElasticSearchServer _esServer;

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
        assertNotNull(_esServer);
        assertNotNull(_esServer.getClient());
    }

}
