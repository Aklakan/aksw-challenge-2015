package org.aksw.challenge;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.aksw.commons.util.compress.MetaBZip2CompressorInputStream;
import org.aksw.commons.util.strings.StringUtils;
import org.aksw.jena_sparql_api.cache.extra.CacheBackend;
import org.aksw.jena_sparql_api.cache.extra.CacheFrontend;
import org.aksw.jena_sparql_api.cache.extra.CacheFrontendImpl;
import org.aksw.jena_sparql_api.core.FluentQueryExecutionFactory;
import org.aksw.jena_sparql_api.core.QueryExecutionFactory;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.jena.atlas.web.HttpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.sparql.core.Prologue;




public class MainAkswChallenge2015 {

    private static final Logger logger = LoggerFactory.getLogger(MainAkswChallenge2015.class);


//    public static void main(String[] args) throws Exception {
//        InputStream in = new FileInputStream(new File("/home/raven/tmp/CleanQueries.txt"));
//
//        String str = StreamUtils.toString(in);
//
//        str = "Query String: foobar ---------";
//        str = str.replaceAll("\n", "xxxxxxxxxxxxxxx");
//
//        System.out.println(str);
//
//        Pattern pattern = Pattern.compile("(foobar)", Pattern.CASE_INSENSITIVE); // | Pattern.DOTALL | Pattern.MULTILINE);
//
//
//
//        System.out.println("matches");
//        Matcher m = pattern.matcher(str);
//        m.matches();
//        while(m.find()) {
//            String x = m.group(1);
//            x = x.replaceAll("xxxxxxxxxxxxxxx", "\n");
//            System.out.println(x);
//        }
//
//    }



    public static void main(String[] args) throws Exception {
        Resource queryLog = new ClassPathResource("trained_queries.txt.bz2");
        MetaBZip2CompressorInputStream in = new MetaBZip2CompressorInputStream(queryLog.getInputStream());

        BufferedReader reader = new BufferedReader(new InputStreamReader(in));

        File cacheDir = new File("cache");
        CacheBackend cacheBackend = new CacheBackendFile(cacheDir, 10000000l);
        CacheFrontend cacheFrontend = new CacheFrontendImpl(cacheBackend);

        QueryExecutionFactory qef = FluentQueryExecutionFactory
            .http("http://localhost:8890/sparql", "http://data.semanticweb.org/")
            .config()
                .withCache(cacheFrontend)
            .end()
            .create();


        int totalQueryCount = 0;
        int parseFailCount = 0;
        Prologue prologue = new Prologue();

        String rawLine;
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

            QueryExecution qe = qef.createQueryExecution(query);
            if(query.isConstructType()) {
                throw new RuntimeException("should not happen");
            } else if(query.isAskType()) {

            } else if(query.isSelectType()) {
                try {
                    ResultSet rs = qe.execSelect();
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
            //qe.abort();

            ++totalQueryCount;
        }

//        logger.info("total: " + totalQueryCount);
//        logger.info("parseFailCount: " + parseFailCount);
//        logger.info("empty result: " + emptyResultCount);
    }
}
