package no.ssb.lds.core.search.solr;

import no.ssb.lds.api.search.SearchIndex;
import no.ssb.lds.api.search.SearchIndexProvider;

import java.util.Map;
import java.util.Set;

public class SolrProvider implements SearchIndexProvider {

    @Override
    public String getProviderId() {
        return "solr";
    }

    @Override
    public Set<String> configurationKeys() {
        return Set.of(
                "search.index.url"
        );
    }

    @Override
    public SearchIndex getSearchIndex(Map<String, String> configuration) {
        return new SolrSearchIndex(configuration.get("search.index.url"));
    }

}
