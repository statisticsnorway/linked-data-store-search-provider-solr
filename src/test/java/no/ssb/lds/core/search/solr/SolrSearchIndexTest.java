package no.ssb.lds.core.search.solr;

import com.fasterxml.jackson.databind.JsonNode;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.json.JsonTools;
import no.ssb.lds.api.search.SearchResponse;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.Set;

import static org.testng.Assert.*;

public class SolrSearchIndexTest {

    private static EmbeddedSolrServer server;
    private SolrSearchIndex searchIndex;

    @BeforeClass
    public void setUp() {
        CoreContainer container = new CoreContainer("src/test/resources/solr");
        server = new EmbeddedSolrServer(container, "lds-index" );
        searchIndex = new SolrSearchIndex(server, server);
        container.load();
    }

    @AfterClass
    public void tearDown() throws Exception {
        Path solrHome = Paths.get("src/test/resources/solr/lds-index/data");
        if (solrHome.toFile().exists()) {
            FileUtils.deleteDirectory(solrHome.toFile());
        }
    }

    @Test
    public void testSearch() throws Exception {
        JsonNode json = JsonTools.toJsonNode(getResourceAsString("UnitDataSet_Person_1.json", StandardCharsets.UTF_8));
        final JsonDocument jsonDocument = new JsonDocument(new DocumentKey("ns", "UnitDataSet", json.get("id").textValue(),
                ZonedDateTime.now()), json);
        searchIndex.createOrOverwrite(jsonDocument).blockingAwait();
        server.commit();

        SearchResponse response = searchIndex.search("Norway", Set.of("UnitDataSet"), 0, 10).blockingGet();
        assertEquals(response.getTotalHits(), 1);
        assertEquals(response.getResults().iterator().next().getDocumentKey().entity(), "UnitDataSet");
        assertEquals(response.getResults().iterator().next().getDocumentKey().id(), "b9c10b86-5867-4270-b56e-ee7439fe381e");

        searchIndex.delete(jsonDocument).blockingAwait();
        server.commit();
        response = searchIndex.search("Norway", null, 0, 10).blockingGet();
        assertEquals(response.getTotalHits(), 0);
    }

    private static String getResourceAsString(String path, Charset charset) {
        try {
            URL systemResource = ClassLoader.getSystemResource(path);
            if (systemResource == null) {
                return null;
            }
            URLConnection conn = systemResource.openConnection();
            try (InputStream is = conn.getInputStream()) {
                byte[] bytes = is.readAllBytes();
                CharBuffer cbuf = CharBuffer.allocate(bytes.length);
                CoderResult coderResult = charset.newDecoder().decode(ByteBuffer.wrap(bytes), cbuf, true);
                if (coderResult.isError()) {
                    coderResult.throwException();
                }
                return cbuf.flip().toString();
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}