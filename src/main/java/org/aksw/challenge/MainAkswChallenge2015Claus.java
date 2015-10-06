package org.aksw.challenge;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.aksw.commons.util.compress.MetaBZip2CompressorInputStream;
import org.aksw.commons.util.strings.StringUtils;
import org.aksw.jena_sparql_api.cache.extra.CacheBackend;
import org.aksw.jena_sparql_api.cache.extra.CacheFrontend;
import org.aksw.jena_sparql_api.cache.extra.CacheFrontendImpl;
import org.aksw.jena_sparql_api.cache.file.CacheBackendFile;
import org.aksw.jena_sparql_api.core.FluentQueryExecutionFactory;
import org.aksw.jena_sparql_api.core.QueryExecutionFactory;
import org.aksw.jena_sparql_api.stmt.SparqlQueryParser;
import org.aksw.jena_sparql_api.stmt.SparqlQueryParserImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.query.Syntax;




public class MainAkswChallenge2015Claus {

    private static final Logger logger = LoggerFactory.getLogger(MainAkswChallenge2015Claus.class);


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


    public static void indexQueries(Iterable<Query> queries, QueryExecutionFactory qef) {
        for(Query query : queries) {
            QueryExecution qe = qef.createQueryExecution(query);
            ResultSet rs = qe.execSelect();
            ResultSetFormatter.consume(rs);
        }
    }

    public static void nodeFreq(Iterable<Query> queries, QueryExecutionFactory qef) throws FileNotFoundException {
        Multiset<Node> nodes = HashMultiset.create();
        int i = 0;
        for(Query query : queries) {
            System.out.println("" + ++i);
            QueryExecution qe = qef.createQueryExecution(query);

            ResultSet rs;
            try {
                rs = qe.execSelect();
            } catch(Exception e) {
                logger.warn("Something went wrong", e);
                continue;
            }

            Multiset<Node> tmp = NodeUtils.getNodes(rs);
            nodes.addAll(tmp);
        }


        PrintWriter out = new PrintWriter(new FileOutputStream(new File("node-freq.dat")));
        for (Node node : Multisets.copyHighestCountFirst(nodes).elementSet()) {
            out.println(nodes.count(node) + "\t" + node);
        }
        out.flush();
        out.close();

    }

    public static Iterable<Query> readQueryLog() throws IOException {
        Resource queryLog = new ClassPathResource("trained_queries.txt.bz2");
        MetaBZip2CompressorInputStream in = new MetaBZip2CompressorInputStream(queryLog.getInputStream());


        BufferedReader reader = new BufferedReader(new InputStreamReader(in));

        SparqlQueryParser queryParser = SparqlQueryParserImpl.create(Syntax.syntaxARQ);

        String rawLine;
        List<Query> result = new ArrayList<Query>();
        while((rawLine = reader.readLine()) != null) {

            String queryStr = StringUtils.urlDecode(rawLine);
            Query query = queryParser.apply(queryStr);

            result.add(query);
        }
        reader.close();

        return result;

    }

    public static void main(String[] args) throws Exception {

        Iterable<Query> queries = readQueryLog();

        File cacheDir = new File("cache");
        CacheBackend cacheBackend = new CacheBackendFile(cacheDir, 10000000l);
        CacheFrontend cacheFrontend = new CacheFrontendImpl(cacheBackend);

        QueryExecutionFactory qef = FluentQueryExecutionFactory
            .http("http://localhost:8890/sparql", "http://data.semanticweb.org/")
            .config()
                .withCache(cacheFrontend)
            .end()
            .create();


        //indexQueries(queries, qef);
        nodeFreq(queries, qef);

    }
}
