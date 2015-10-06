package org.aksw.challenge;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.AbstractIterator;

public class FoldIterator<T>
    extends AbstractIterator<Fold<T>>
{
    private List<? extends List<T>> partitions;
    private int pick = 0;

    public FoldIterator(List<? extends List<T>> partitions) {
        this.partitions = partitions;
    }


    @Override
    protected Fold<T> computeNext() {
        if(pick >= partitions.size()) {
            return endOfData();
        }

        List<T> train = new ArrayList<T>();
        for(int i = 0 ; i < partitions.size(); ++i) {
            if(i == pick) {
                continue;
            }

            Collection<T> tmp = partitions.get(i);
            train.addAll(tmp);
        }

        List<T> validate = partitions.get(pick);
        ++pick;

        Fold<T> result = new Fold<T>(train, validate);

        return result;
    }

}