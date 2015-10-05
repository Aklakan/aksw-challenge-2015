package org.aksw.challenge;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.aksw.commons.util.compress.MetaBZip2CompressorInputStream;
import org.aksw.commons.util.strings.StringUtils;
import org.aksw.jena_sparql_api.cache.extra.CacheBackend;
import org.aksw.jena_sparql_api.cache.extra.CacheFrontend;
import org.aksw.jena_sparql_api.cache.extra.CacheFrontendImpl;
import org.aksw.jena_sparql_api.cache.staging.CacheBackendDaoPostgres;
import org.aksw.jena_sparql_api.cache.staging.CacheBackendDataSource;
import org.aksw.jena_sparql_api.core.FluentQueryExecutionFactory;
import org.aksw.jena_sparql_api.core.QueryExecutionFactory;
import org.apache.jena.atlas.web.HttpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.sparql.core.Prologue;

public class MainAkswChallenge2015 {

    private static final Logger logger = LoggerFactory.getLogger(MainAkswChallenge2015.class);

    public static void main(String[] args) throws Exception {
        Resource queryLog = new ClassPathResource("trained_queries.txt.bz2");
        MetaBZip2CompressorInputStream in = new MetaBZip2CompressorInputStream(queryLog.getInputStream());

        BufferedReader reader = new BufferedReader(new InputStreamReader(in));

        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUrl("jdbc:postgresql://localhost:5432/facete2tomcatcommon");
        ds.setUsername("postgres");
        ds.setPassword("postgres");

//        boolean useBoneCpWrapper = false;
//        if(useBoneCpWrapper) {
//
//            BoneCPConfig cpConfig = new BoneCPConfig();
//            cpConfig.setDatasourceBean(dsBean);
//
//            cpConfig.setMinConnectionsPerPartition(1);
//            cpConfig.setMaxConnectionsPerPartition(10);
//            cpConfig.setPartitionCount(2);
//            //cpConfig.setCloseConnectionWatch(true);
//
//            try {
//                result = new BoneCPDataSource(cpConfig);
//            } catch(Exception e) {
//                throw new RuntimeException(e);
//            }
//        } else {
//            result = dsBean;
//        }


//        PG
//
        CacheBackend cacheBackend = new CacheBackendDataSource(ds, new CacheBackendDaoPostgres(1000000l));
        CacheFrontend cacheFrontend = new CacheFrontendImpl(cacheBackend);

        QueryExecutionFactory qef = FluentQueryExecutionFactory
            .http("http://localhost:8890/sparql", "http://data.semanticweb.org/")
            .config()
                .withCache(cacheFrontend)
            .end()
            .create();

        Prologue prologue = new Prologue();
        //SparqlQueryParser parser = SparqlQueryParserImpl.create(Syntax.syntaxARQ, prologue);


        int parseFailCount = 0;
        int totalQueryCount = 0;
        int emptyResultCount = 0;


        String rawLine;

        Map<Node, Integer> map = new HashMap<Node, Integer>();

        while((rawLine = reader.readLine()) != null) {
            System.out.println("Processing Query #" + totalQueryCount);

            String queryStr = StringUtils.urlDecode(rawLine);

            Query query;
            try {
                query = QueryFactory.create(queryStr);
                //query = parser.apply(queryStr);
            } catch(Exception e) {
                ++parseFailCount;
                continue;
            }


            //String hash = StringUtils.md5Hash();
            //File file = new File("resultset" + totalQueryCount);


            QueryExecution qe = qef.createQueryExecution(query);
            if(query.isConstructType()) {
                throw new RuntimeException("should not happen");
            } else if(query.isAskType()) {

            } else if(query.isSelectType()) {
                try {
                    ResultSet rs = qe.execSelect();
                    if(!rs.hasNext()) {
                        ++emptyResultCount;
                    }

                    ResultSetFormatter.consume(rs);
                } catch(Exception e) {
                    if(e instanceof HttpException) {
                        HttpException x = (HttpException)e;
                        logger.warn("Error on " + query);
                        logger.warn(x.getResponse());
                    }
                }
            } else if(query.isDescribeType()) {

            }
            qe.abort();


            ++totalQueryCount;
        }

        logger.info("total: " + totalQueryCount);
        logger.info("parseFailCount: " + parseFailCount);
        logger.info("empty result: " + emptyResultCount);
    }
}
