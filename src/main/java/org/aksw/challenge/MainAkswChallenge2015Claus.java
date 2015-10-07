package org.aksw.challenge;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

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

    public static void findQueriesWithNonEmptyResult(Iterable<Query> queries, QueryExecutionFactory qef) throws IOException {
        PrintWriter out = new PrintWriter(new BZip2CompressorOutputStream(new FileOutputStream(new File("cleaned-queries.dat.bz2"))));

        for(Query query : queries) {
            boolean hasBindings;
            QueryExecution qe = qef.createQueryExecution(query);
            try {
                ResultSet rs = qe.execSelect();
                hasBindings = rs.hasNext();
                ResultSetFormatter.consume(rs);

                if(hasBindings) {
                    out.println(StringUtils.urlEncode("" + query));
                }
            } catch(Exception e) {
                logger.warn("Something went wrong", e);
                continue;
            }
        }
        out.flush();
        out.close();
    }


    public static void indexQueries(Iterable<Query> queries, QueryExecutionFactory qef) {
        for(Query query : queries) {
            QueryExecution qe = qef.createQueryExecution(query);
            ResultSet rs = qe.execSelect();
            ResultSetFormatter.consume(rs);
        }
    }

    public static Map<String, Integer> createFreqMap(Iterable<Query> queries, QueryExecutionFactory qef) {
        Multiset<Node> nodes = HashMultiset.create();
        int i = 0;
        for(Query query : queries) {
            //System.out.println("createFreqMap" + ++i);
            QueryExecution qe = qef.createQueryExecution(query);

            ResultSet rs;
            try {
                rs = qe.execSelect();
            } catch(Exception e) {
                logger.warn("Something went wrong", e);
                continue;
            }

            Multiset<Node> tmp = NodeUtils.getNodes(rs);

            boolean singleCount = true;
            if(singleCount) {
                Set<Node> x = new HashSet<Node>(tmp);
                nodes.addAll(x);
            } else {
                nodes.addAll(tmp);
            }
        }


        Map<String, Integer> result = new HashMap<String, Integer>();
        for (Node node : Multisets.copyHighestCountFirst(nodes).elementSet()) {
            int count = nodes.count(node);
            if(node.isURI()) {
                String str = node.getURI();
                result.put(str, count);
            }
        }

        //PrintWriter out = new PrintWriter(new FileOutputStream(new File("node-query-freq.dat")));
        return result;
    }

    public static List<Query> readQueryLog() throws IOException {
        Resource queryLog = new ClassPathResource("queries-with-results.dat.bz2");
        MetaBZip2CompressorInputStream in = new MetaBZip2CompressorInputStream(queryLog.getInputStream());

        BufferedReader reader = new BufferedReader(new InputStreamReader(in));

        SparqlQueryParser queryParser = SparqlQueryParserImpl.create(Syntax.syntaxARQ);

        String rawLine;
        List<Query> result = new ArrayList<Query>();
        while((rawLine = reader.readLine()) != null) {

            String queryStr = StringUtils.urlDecode(rawLine);
            Query query = queryParser.apply(queryStr);

//            System.out.println(query);
//            System.out.println("--------------------------");

            result.add(query);
        }
        reader.close();

        return result;

    }


    public static <T> FoldCollection<T> createFoldCollection(List<T> items, int k) {
        int partitionSize = (int)(items.size() / (double)k);
        List<List<T>> partitions = Lists.newArrayList(Iterables.partition(items, partitionSize));

        FoldCollection<T> result = new FoldCollection<T>(partitions);
        return result;
    }


    public static void doCrossValidation(List<Query> queries, QueryExecutionFactory qef) {

        Collections.shuffle(queries);
        FoldCollection<Query> folds = createFoldCollection(queries, 10);

        int x = 0;
        for(Fold<Query> fold : folds) {
            ++x;

            Map<String, Integer> candScores = createFreqMap(fold.getTrain(), qef);
            Map<String, Integer> refScores = createFreqMap(fold.getValidate(), qef);

            double rmsd = Eval.getRMSD(candScores, refScores);

            System.out.println("Processing in fold " + x + ": " + rmsd);
        }

    }

    public static void createFreqList(List<Query> queries, QueryExecutionFactory qef) throws FileNotFoundException {
        Map<String, Integer> candScores = createFreqMap(queries, qef);
        List<String> items = Eval.createOrderedList(candScores);
        PrintStream out = new PrintStream(new File("group2-final-list.txt"));

        for(String item : items) {
            out.println(item);
        }
        out.flush();
        out.close();
    }


    public static void main(String[] args) throws Exception {

        Model model = ModelFactory.createDefaultModel();

//        InputStream in = new MetaBZip2CompressorInputStream(new ClassPathResource("swdf.nt.bz2").getInputStream());
//        model.read(in, "http://example.org/base/", "N-TRIPLES");

//        Number x = new Double(4.5);
//        System.out.println(x.intValue());

//        System.exit(0);

        List<Query> queries = readQueryLog();

        File cacheDir = new File("cache");
        CacheBackend cacheBackend = new CacheBackendFile(cacheDir, 10000000l);
        CacheFrontend cacheFrontend = new CacheFrontendImpl(cacheBackend);

        QueryExecutionFactory qef = FluentQueryExecutionFactory
            .http("http://localhost:8890/sparql", "http://data.semanticweb.org/")
            //.model(model)
            .config()
                .withCache(cacheFrontend)
            .end()
            .create();



        List<Query> subList = queries.subList(3700, queries.size());
        createFreqList(subList, qef);
        //doCrossValidation(queries, qef);
        //findQueriesWithNonEmptyResult(queries, qef);
        //indexQueries(queries, qef);
//        Map<String, Integer> map = createFreqMap(queries, qef);
//        TreeMultimap<Integer, String> mm = Eval.indexByScore(map);
//        Eval.print(System.out, mm, 10, false);

    }

    public static void core() {



    }
}
