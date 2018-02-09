package com.github.joraclista.scanner.strategies;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedScanList;
import com.github.joraclista.api.DynamoDbAPI;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.PaginationLoadingStrategy.LAZY_LOADING;

/**
 * Created by Alisa
 * version 1.0.
 */
public abstract class BaseScanStrategy<T, E, R> {

    private final DynamoDbAPI dynamoClient;
    private final Class<T> tableMappingClass;
    protected int itemsPerScan;
    protected int pauseBetweenScans;
    protected Function<T, E> itemsMappingFunction;
    protected Consumer<E> itemsConsumer;
    protected Consumer<List<E>> itemsBatchConsumer;

    public BaseScanStrategy(DynamoDbAPI dynamoClient,
                            Class<T> tableMappingClass,
                            int itemsPerScan,
                            int pauseBetweenScans,
                            Function<T, E> itemsMappingFunction) {
        this.dynamoClient = dynamoClient;
        this.tableMappingClass = tableMappingClass;
        this.itemsPerScan = itemsPerScan;
        this.pauseBetweenScans = pauseBetweenScans;
        this.itemsMappingFunction = itemsMappingFunction;
    }


    protected PaginatedScanList<T> getPaginatedList() {
        DynamoDBScanExpression scanExpression = new DynamoDBScanExpression()
                .withLimit(itemsPerScan);

        return dynamoClient.getMapper().scan(tableMappingClass, scanExpression, new DynamoDBMapperConfig.Builder().withPaginationLoadingStrategy(LAZY_LOADING)
                .build());
    }

    public abstract R scan() throws InterruptedException;
}
