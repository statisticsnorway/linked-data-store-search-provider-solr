package no.ssb.lds.core.search.solr;

import io.reactivex.Completable;
import io.reactivex.Single;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.flattened.FlattenedDocument;
import no.ssb.lds.api.persistence.flattened.FlattenedDocumentLeafNode;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.json.JsonToFlattenedDocument;
import no.ssb.lds.api.persistence.streaming.FragmentType;
import no.ssb.lds.api.search.SearchIndex;
import no.ssb.lds.api.search.SearchResponse;
import no.ssb.lds.api.search.SearchResult;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.HighlightParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A Solr implementation of {@link SearchIndex} that will execute queries and updates to Solr server via HTTP.
 * The environment property @{code search.index.url} must be provided.
 */
public class SolrSearchIndex implements SearchIndex {

    private final SolrClient solrUpdateClient;
    private final SolrClient solrQueryClient;
    private static final Logger LOG = LoggerFactory.getLogger(SolrSearchIndex.class);
    private static final String DEFAULT_SEARCH_FIELD = "searchfield";
    private static final String ENTITY_FIELD = "entity";

    public SolrSearchIndex(String urlString) {
        this(new ConcurrentUpdateSolrClient.Builder(urlString).build(), new HttpSolrClient.Builder(urlString).build());
    }

    SolrSearchIndex(SolrClient solrUpdateClient, SolrClient solrQueryClient) {
        LOG.info("Initializing SolrSearchIndex");
        this.solrUpdateClient = solrUpdateClient;
        this.solrQueryClient = solrQueryClient;
    }

    @Override
    public Completable createOrOverwrite(JsonDocument jsonDocument) {
        return createOrOverwrite(Arrays.asList(jsonDocument));
    }

    @Override
    public Completable createOrOverwrite(Collection<JsonDocument> jsonDocuments) {
        return Completable.fromAction(() -> {
            List<SolrInputDocument> updateDocs = new ArrayList<>();
            for (JsonDocument jsonDocument : jsonDocuments) {
                final SolrInputDocument doc = new SolrInputDocument();
                DocumentKey key = jsonDocument.key();
                JsonToFlattenedDocument converter = new JsonToFlattenedDocument(key.namespace(), key.entity(), key.id(),
                        key.timestamp(), jsonDocument.jackson(), Integer.MAX_VALUE);
                addFields(converter.toDocument(), doc);
                doc.addField(CommonParams.ID, getIdString(jsonDocument.key()));
                doc.addField(ENTITY_FIELD, jsonDocument.key().entity());
                updateDocs.add(doc);
            }
            this.solrUpdateClient.add(updateDocs, 5000);
            LOG.info("Added {} documents to index", jsonDocuments.size());
        });
    }

    @Override
    public Completable delete(JsonDocument jsonDocument) {
        return Completable.fromAction(() -> {
            final String idString = getIdString(jsonDocument.key());
            this.solrUpdateClient.deleteById(idString, 5000);
            LOG.info("Finished deleting : " + idString);
        });
    }

    @Override
    public Completable deleteAll() {
        return Completable.fromAction(() -> {
            this.solrUpdateClient.deleteByQuery("*:*");
            this.solrUpdateClient.commit();
        });
    }

    @Override
    public Single<SearchResponse> search(String text, List<String> typeFilters, long from, long size) {
        try {
            SolrQuery query = new SolrQuery().setStart(Long.valueOf(from).intValue())
                    .setRows(Long.valueOf(size).intValue());
            query.set(CommonParams.Q, text + createFilters(typeFilters));
            query.set(CommonParams.DF, DEFAULT_SEARCH_FIELD);
            query.setHighlight(true);
            query.set(HighlightParams.FIELDS, "*languageText");
            LOG.info("Executing query: " + query.toString());
            SolrDocumentList response = this.solrQueryClient.query(query).getResults();
            LOG.info("Number of hits: " + response.getNumFound());
            List<SearchResult> results = response.stream()
                    .map(result -> new SearchResult(fromIdString(result.get("id").toString())))
                    .collect(Collectors.toList());
            return Single.just(new SearchResponse(response.getNumFound(), results, from, size));
        } catch (Exception e) {
            LOG.error("An error ocurred", e);
            throw new RuntimeException(e);
        }
    }

    private String createFilters(List<String> typeFilters) {
        if (typeFilters != null && !typeFilters.isEmpty()) {
            return " AND (" + typeFilters.stream().map(name -> "entity:" + name).collect(Collectors.joining(" OR ")) + ")";
        } else {
            return "";
        }
    }


    private void addFields(FlattenedDocument node, SolrInputDocument doc) {
        for (Map.Entry<String, FlattenedDocumentLeafNode> entry : node.leafNodesByPath().entrySet()) {
            if (entry.getValue().type() == FragmentType.STRING && entry.getKey().contains("languageText")) {
                doc.addField(entry.getKey(), entry.getValue().value());
                doc.addField(DEFAULT_SEARCH_FIELD, entry.getValue().value());
            }
        }
    }

    private String getIdString(DocumentKey key) {
        return String.format("%s/%s/%s/%s",
                key.namespace(), key.entity(), key.id(), key.timestamp()
        );
    }


    private DocumentKey fromIdString(String id) {
        String[] splitted = id.split("/");
        ZonedDateTime version = ZonedDateTime.parse(splitted[3] + "/" + splitted[4], DateTimeFormatter.ISO_ZONED_DATE_TIME);
        return new DocumentKey(splitted[0],
                splitted[1],
                splitted[2], version);
    }

}
