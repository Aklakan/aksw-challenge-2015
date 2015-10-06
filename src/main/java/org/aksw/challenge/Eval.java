package org.aksw.challenge;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;

/**
 * Evaluation method for the AKSW Challenge 2015
 * @author ngonga
 */
public class Eval {

    public static void print(PrintStream out, TreeMultimap<Integer, String> scoreToUris, int n, boolean reverse) {
        int i = 0;
        for(Entry<Integer, Collection<String>> entry : scoreToUris.asMap().entrySet()) {
            if(i >= n) {
                break;
            }

            out.println(entry.getKey() + "\t" + entry.getValue());

            ++i;
        }
    }

    public static double getRMSD(Map<String, Integer> uriToScore, Map<String, Integer> testScoreList) {

        System.out.println();
        System.out.println("RMSD ------------");
        TreeMultimap<Integer, String> a = indexByScore(uriToScore);
        print(System.out, a, 10, false);
        System.out.println("----");
        TreeMultimap<Integer, String> b = indexByScore(testScoreList);
        print(System.out, b, 10, false);


        Map<String, List<Double>> rankRange = createRanks(uriToScore);
        List<String> testList = Eval.createOrderedList(testScoreList);

        double result = getRMSD(testList, rankRange);
        return result;
    }

    /**
     * Implementation of RMSD.
     * @param response Output of an algorithm, read from a file.
     * @param rankRange Rank ranges computed by executing real queries. Map a
     * resource to a list with allowed begin and end rank
     * @return The RMSD score.
     */
    public static double getRMSD(List<String> response, Map<String, List<Double>> rankRange)
    {
        int size = rankRange.keySet().size();
        //Set of foundResources returned by tool
        Set<String> foundResources = new HashSet<>();

        double error = 0d;
        String resource;

        for(int i=0; i<response.size(); i++)
        {
            resource = response.get(i);
            //remember seen foundResources
            foundResources.add(resource);

            //compute error for this resource
            //unknown foundResources will not be ranked
            if(rankRange.containsKey(resource))
            {
                //range contains min and max rank
                List<Double> range = rankRange.get(resource);
                //if resource not within acceptable range then increase total error
                if(!(i >= range.get(0) && i <= range.get(1)))
                {
                    error = error + Math.min(Math.pow(i-range.get(0), 2),
                            Math.pow(i-range.get(1), 2));
                }
            }
        }

        //now check for foundResources that were not ranked and assign them the max error
        for(String r: rankRange.keySet())
        {
            if(!foundResources.contains(r))
                error = error + Math.pow(size, 2);
        }
        return Math.sqrt(error);
    }


    private static TreeMultimap<Integer, String> indexByScore(Map<String, Integer> uriToScore) {
        TreeMultimap<Integer, String> scoreToUris = TreeMultimap.create(Ordering.natural().reverse(), Ordering.natural());
        for(Entry<String, Integer> entry : uriToScore.entrySet()) {
            scoreToUris.put(entry.getValue(), entry.getKey());
        }
        return scoreToUris;
    }

    public static List<String> createOrderedList(Map<String, Integer> uriToScore) {
        TreeMultimap<Integer, String> scoreToUris = indexByScore(uriToScore);

        List<String> result = new ArrayList<>(scoreToUris.values());
        return result;
    }

    public static Map<String, List<Double>> createRanks(Map<String, Integer> uriToScore) {
        TreeMultimap<Integer, String> scoreToUris = indexByScore(uriToScore);

        Map<String, List<Double>> result = new HashMap<>();
//ModelUtils.
//        Model m;
//        m.createProperty(node.getUri());
        int i = 0;
        for(Entry<Integer, Collection<String>> entry : scoreToUris.asMap().entrySet()) {
            Collection<String> uris = entry.getValue();
            int k = uris.size();

            List<Double> range = new ArrayList<>();
            range.add((double)i);
            range.add((double)(i + k - 1));

            for(String uri : uris) {
                result.put(uri, range);
            }

            i += k;
        }

        return result;
    }

    /**
     * Test for RMSD. Expected result is sqrt(2).
     */
    public static void test()
    {
        List<String> response = new ArrayList<>();
        response.add("B");
        response.add("A");
        response.add("C");
        response.add("D");
        response.add("E");

        Map<String, List<Double>> rankRange = new HashMap<>();
        List<Double> zero = new ArrayList<>(); zero.add(0d); zero.add(0d);
        List<Double> one = new ArrayList<>(); one.add(1d); one.add(1d);
        List<Double> two = new ArrayList<>(); two.add(2d); two.add(2d);
        List<Double> threeFour = new ArrayList<>(); threeFour.add(3d); threeFour.add(4d);

        rankRange.put("A", zero);
        rankRange.put("B", one);
        rankRange.put("C", two);
        rankRange.put("D", threeFour);
        rankRange.put("E", threeFour);

        System.out.println(getRMSD(response, rankRange));
    }

    public static void main(String args[])
    {
       test();
    }
}
