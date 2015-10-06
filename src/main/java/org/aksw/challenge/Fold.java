package org.aksw.challenge;

import java.util.List;

class Fold<T> {
    private List<T> train;
    private List<T> validate;

    public Fold(List<T> train, List<T> validate) {
        super();
        this.train = train;
        this.validate = validate;
    }

    public List<T> getTrain() {
        return train;
    }

    public List<T> getValidate() {
        return validate;
    }
}