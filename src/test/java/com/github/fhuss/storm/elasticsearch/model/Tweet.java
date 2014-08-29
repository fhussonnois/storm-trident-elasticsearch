package com.github.fhuss.storm.elasticsearch.model;

import java.io.Serializable;

/**
 * Simple class used for testing purpose.
 */
public class Tweet implements Serializable {

    private String text;
    private int count;

    public Tweet() {
    }

    public Tweet(String text, int count) {
        this.text = text;
        this.count = count;
    }

    public String getText() {
        return text;
    }

    public int getCount() {
        return count;
    }

    public void incrementCount( ) {
        this.count++;
    }

    public String toString() {
        return text;
    }
}
