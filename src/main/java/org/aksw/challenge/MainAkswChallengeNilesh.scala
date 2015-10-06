package org.aksw.challenge

import java.io.{FileOutputStream, PrintWriter, FileNotFoundException, File}
import javax.management.Query

import com.google.common.collect.{Multisets, HashMultiset, Iterables}
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.query.{ResultSet, QueryExecution}
import com.hp.hpl.jena.rdf.model.{ModelFactory, Model}
import org.aksw.jena_sparql_api.cache.extra.{CacheFrontendImpl, CacheFrontend, CacheBackend}
import org.aksw.jena_sparql_api.cache.file.CacheBackendFile
import org.aksw.jena_sparql_api.core.{FluentQueryExecutionFactory, QueryExecutionFactory}
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Created by nilesh on 06/10/15.
 */
class MainAkswChallengeNilesh {
  def main(args: Array[String]) = {
//    val claus = new MainAkswChallenge2015Claus
    val queries = MainAkswChallenge2015Claus.readQueryLog().toSet
//    val foo = mutable.Set[Query]()
//    val foo: Set[Query] = new HashSet[Query]

//    Iterables.addAll(foo, queries)
    System.out.println(queries.size)
    //        System.exit(0);
    val cacheDir: File = new File("cache")
    val cacheBackend: CacheBackend = new CacheBackendFile(cacheDir, 10000000l)
    val cacheFrontend: CacheFrontend = new CacheFrontendImpl(cacheBackend)

    val model: Model = ModelFactory.createDefaultModel
    model.read("/Users/nilesh/swdf.nt")

    //        model.listst
    val qef: QueryExecutionFactory = FluentQueryExecutionFactory.model(model).config.withCache(cacheFrontend).end.create




    //indexQueries(queries, qef);
    MainAkswChallenge2015Claus.nodeFreq(queries, qef)
  }

  def nodeFreq(queries: Iterable[Query], qef: QueryExecutionFactory) {
    val nodes = mutable.Map[Node, Int]()
    var i = 0
    for (query <- queries) {
      i += 1
      println("Read query " + i)
      val qe: QueryExecution = qef.createQueryExecution(query)
      var rs: ResultSet = null
      try {
        rs = qe.execSelect
      }
      catch {
        case e: Exception => {
          logger.warn("Something went wrong", e)
          continue //todo: continue is not supported
        }
      }
      val tmp: Multiset[Node] = NodeUtils.getNodes(rs)
      val singleCount: Boolean = false
      if (singleCount) {
        val x: Set[Node] = new HashSet[Node](tmp)
        nodes.addAll(x)
      }
      else {
        nodes.addAll(tmp)
      }
    }
    val out: PrintWriter = new PrintWriter(new FileOutputStream(new File("node-query-freq.dat")))
    import scala.collection.JavaConversions._
    for (node <- Multisets.copyHighestCountFirst(nodes).elementSet) {
      out.println(nodes.count(node) + "\t" + node)
    }
    out.flush
    out.close
  }
}
