package org.aksw.challenge;

import java.util.Iterator;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;

public class NodeUtils {

    public static Multiset<Node> getNodes(Binding binding) {
        Multiset<Node> result = HashMultiset.create();
        Iterator<Var> it = binding.vars();
        while(it.hasNext()) {
            Var var = it.next();
            Node node = binding.get(var);
            result.add(node);
        }
        return result;
    }

    public static Multiset<Node> getNodes(ResultSet rs) {
        Multiset<Node> result = HashMultiset.create();

        while(rs.hasNext()) {
            Binding binding = rs.nextBinding();
            Multiset<Node> tmp = getNodes(binding);
            result.addAll(tmp);
        }
        return result;
    }
}
