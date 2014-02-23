package com.github.fhuss.storm.elasticsearch;

/**
 * This class should be used to wrap data required to index a document.
 *
 * @author fhussonnois
 * @param <T> type of the underlying document
 */
public class Document<T> {

    /**
     * The name of the index
     */
    private String name;
    /**
     * The type of document
     */
    private String type;
    /**
     * The source document
     */
    private T source;
    /**
     * The document id
     */
    private String id;
    /**
     * The parent document id
     */
    private String parentId;

    public Document(String name, String type,T source) {
        this(name, type, source, null, null);
    }

    public Document(String name, String type, T source, String id) {
        this(name, type, source, id, null);
    }

    public Document(String name, String type, T source, String id, String parentId) {
        this.name = name;
        this.type = type;
        this.source = source;
        this.id = id;
        this.parentId = parentId;
    }

    public String getName( ) {
        return this.name;
    }

    public String getType( ) {
        return this.type;
    }

    public T getSource( ) {
        return this.source;
    }

    public String getId( ) {
        return this.id;
    }

    public String getParentId() {
        return this.parentId;
    }
}
