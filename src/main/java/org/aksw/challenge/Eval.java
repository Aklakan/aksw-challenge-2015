package org.aksw.challenge;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Evaluation method for the AKSW Challenge 2015
 * @author ngonga
 */
public class Eval {

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
