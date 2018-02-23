package com.github.joraclista.scanner.strategies;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedScanList;
import com.github.joraclista.api.DynamoDbAPI;

import java.util.List;
import java.util.function.Consumer;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.PaginationLoadingStrategy.LAZY_LOADING;

/**
 * Created by Alisa
 * version 1.0.
 */
public abstract class BaseScanStrategy<T, R> {

    private final DynamoDbAPI dynamoClient;
    private final Class<T> tableMappingClass;
    protected int itemsPerScan;
    protected int pauseBetweenScans;
    protected Consumer<T> itemsConsumer;
    protected Consumer<List<T>> itemsBatchConsumer;

    public BaseScanStrategy(DynamoDbAPI dynamoClient,
                            Class<T> tableMappingClass,
                            int itemsPerScan,
                            int pauseBetweenScans) {
        this.dynamoClient = dynamoClient;
        this.tableMappingClass = tableMappingClass;
        this.itemsPerScan = itemsPerScan;
        this.pauseBetweenScans = pauseBetweenScans;
    }


    protected PaginatedScanList<T> getPaginatedList() {
        DynamoDBScanExpression scanExpression = new DynamoDBScanExpression()
                .withLimit(itemsPerScan);

        return dynamoClient.getMapper().scan(tableMappingClass,
                scanExpression,
                new DynamoDBMapperConfig.Builder()
                        .withPaginationLoadingStrategy(LAZY_LOADING)
                        .build());
    }

    public abstract R scan() throws InterruptedException;
}
