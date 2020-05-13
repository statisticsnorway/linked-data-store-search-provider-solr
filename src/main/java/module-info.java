import no.ssb.lds.api.search.SearchIndexProvider;
import no.ssb.lds.core.search.solr.SolrProvider;

module no.ssb.lds.search.provider.solr {
    requires no.ssb.lds.search.api;
    requires no.ssb.lds.persistence.api;
    requires solr.solrj;
    requires org.slf4j;
    requires io.reactivex.rxjava2;

    provides SearchIndexProvider with SolrProvider;
}
