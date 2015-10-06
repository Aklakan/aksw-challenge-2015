package org.aksw.challenge;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.List;

public class FoldCollection<T>
    extends AbstractCollection<Fold<T>>
{
    private List<? extends List<T>> partitions;

    public FoldCollection(List<? extends List<T>> partitions) {
        this.partitions = partitions;
    }

    @Override
    public Iterator<Fold<T>> iterator() {
        Iterator<Fold<T>> result = new FoldIterator<T>(partitions);
        return result;
    }

    @Override
    public int size() {
        int result = partitions.size();
        return result;
    }

}
