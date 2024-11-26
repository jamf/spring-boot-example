package com.zecops.example.neo4j;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zecops.example.ExampleApplication;
import com.zecops.example.neo4j.model.Dirlist;
import com.zecops.example.neo4j.model.Entry;
import org.apache.tomcat.util.buf.HexUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.security.servlet.UserDetailsServiceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.HexFormat;
import java.util.Map;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK, classes = ExampleApplication.class)
@EnableAutoConfiguration(exclude = {UserDetailsServiceAutoConfiguration.class})
@TestPropertySource("classpath:application-test.properties") // Spring gives priority to properties outside the jar regardless of profile, so need to explicitly set this.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class Neo4jAdapterServiceTest {

    @Autowired
    private Neo4jAdapterService neo4jAdapterService;
    @Autowired
    private ObjectMapper jacksonObjectMapper;

    @Test
    public void testProcessDoc() throws IOException {
        Map<String,Object> doc = jacksonObjectMapper.readValue(getClass().getResourceAsStream("/dirlist.json"), new TypeReference<Map<String, Object>>() {});
        Dirlist dirlist = ReflectionTestUtils.invokeMethod(neo4jAdapterService, "processDoc", doc);
        Assertions.assertNotNull(dirlist);
        // Count all entries recursively
        Assertions.assertEquals(1749, countEntries(dirlist.getRootEntry()));
    }

    private int countEntries(Entry entry) {
        if (CollectionUtils.isEmpty(entry.getEntries())) {
            return 1;
        }
        Assertions.assertTrue(entry.getIsFolder());
        int sum = 1;
        for (Entry e : entry.getEntries()) {
            sum += countEntries(e);
        }
        return sum;
    }

}
