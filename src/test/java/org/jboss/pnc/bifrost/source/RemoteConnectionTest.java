package org.jboss.pnc.bifrost.source;

import jdk.nashorn.internal.ir.annotations.Ignore;
import org.apache.http.HttpEntity;
import org.apache.http.entity.BasicHttpEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.jboss.pnc.bifrost.test.DebugTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

/**
 * @author <a href="mailto:matejonnet@gmail.com">Matej Lazar</a>
 */
@DebugTest
public class RemoteConnectionTest {

    private static Logger logger = LoggerFactory.getLogger(RemoteConnectionTest.class);

    private static RestClient lowLevelRestClient;

    private static String[] indexes = new String[] { "test" };

    private static ElasticSearchConfig elasticSearchConfig = ElasticSearchConfig.newBuilder()
            .hosts("http://localhost:9200")
            .indexes(Arrays.asList(indexes).toString())
            .build();


    @BeforeAll
    public static void beforeTest() throws Exception {
        lowLevelRestClient = new ClientFactory(elasticSearchConfig).getConnectedClient();
        logger.info("Connected.");

        String index = elasticSearchConfig.getIndexes().split(",")[0];
        HttpEntity entity = new BasicHttpEntity();
        Request request = new Request("GET", "/" + index + "/_search/", Collections.emptyMap(), entity );
        Response response = lowLevelRestClient.performRequest(request);
        assert 200 == response.getStatusLine().getStatusCode();
    }

    @AfterAll
    public static void after() throws IOException {
        lowLevelRestClient.close();
    }

    @Test @Ignore
    public void shouldConnectToRemoteCluster() throws Exception {
//        ElasticSearch elasticSearch = new ElasticSearch(elasticSearchConfig);
//        DataProvider provider = new DataProvider(elasticSearch);
//        List<Line> lines = provider.get("", "", Optional.empty(), Direction.DESC, 10);
//        lines.forEach(line -> logger.info("line: " + line));
    }
}