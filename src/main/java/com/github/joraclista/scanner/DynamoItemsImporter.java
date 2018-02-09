package com.github.joraclista.scanner;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.github.joraclista.api.DynamoDbAPI;
import com.github.joraclista.scanner.strategies.BaseScanStrategy;
import com.github.joraclista.scanner.strategies.ConsumeBatchScanStrategy;
import com.github.joraclista.scanner.strategies.ConsumeOneByOneScanStrategy;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by Alisa
 * version 1.0.
 */
@Slf4j
public class DynamoItemsImporter<T, E> {

    private static final int ITEMS_PER_DYNAMO_SCAN = 200;
    private static final int PAUSE_BETWEEN_DYNAMO_SCANS_IN_MS = 50;

    private final DynamoDbAPI dynamoClient;
    private final Class<T> tableMappingClass;

    private int itemsPerScan;
    private int pauseBetweenScans;
    private Function<T, E> itemsMappingFunction;


    public DynamoItemsImporter(Regions regions,
                               Class<T> tableMappingClass) {
        if (tableMappingClass.getAnnotationsByType(DynamoDBTable.class).length == 0) {
            throw new IllegalArgumentException("Mapping class '" + tableMappingClass.getName() + "' should be annotated with @DynamoDBTable annotation");
        }
        String tableName = tableMappingClass.getAnnotationsByType(DynamoDBTable.class)[0].tableName();
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("@DynamoDBTable annotation should have valid tableName");
        }
        this.dynamoClient = new DynamoDbAPI(regions);
        this.tableMappingClass = tableMappingClass;
    }

    public DynamoItemsImporter withPauseBetweenScans(Integer pauseBetweenScans) {
        this.pauseBetweenScans = pauseBetweenScans == null ? PAUSE_BETWEEN_DYNAMO_SCANS_IN_MS : pauseBetweenScans;
        return this;
    }

    public DynamoItemsImporter withItemsPerScan(Integer itemsPerScan) {
        this.itemsPerScan = itemsPerScan == null ? ITEMS_PER_DYNAMO_SCAN : itemsPerScan;
        return this;
    }

    public DynamoItemsImporter withItemsMappingFunction(Function<T, E> itemsMappingFunction) {
        this.itemsMappingFunction = itemsMappingFunction;
        return this;
    }


    public List<E> getTableData() {
       return scan(ReadScanStrategy.<T, E>builder()
                .dynamoClient(dynamoClient)
                .tableMappingClass(tableMappingClass)
                .itemsPerScan(itemsPerScan)
                .pauseBetweenScans(pauseBetweenScans)
                .itemsMappingFunction(itemsMappingFunction)
                .build());
    }

    public void consumeTableData(Consumer<E> itemsConsumer) {
        scan(ConsumeOneByOneScanStrategy.<T, E>builder()
                .dynamoClient(dynamoClient)
                .tableMappingClass(tableMappingClass)
                .itemsPerScan(itemsPerScan)
                .pauseBetweenScans(pauseBetweenScans)
                .itemsMappingFunction(itemsMappingFunction)
                .itemsConsumer(itemsConsumer)
                .build());
    }

    public void consumeBatchTableData(Consumer<List<E>> itemsBatchConsumer) {
        scan(ConsumeBatchScanStrategy.<T, E>builder()
                .dynamoClient(dynamoClient)
                .tableMappingClass(tableMappingClass)
                .itemsPerScan(itemsPerScan)
                .pauseBetweenScans(pauseBetweenScans)
                .itemsMappingFunction(itemsMappingFunction)
                .itemsBatchConsumer(itemsBatchConsumer)
                .build());
    }

    @SneakyThrows(InterruptedException.class)
    private <R> R scan(BaseScanStrategy<T, E, R> scanStrategy) {
        return scanStrategy.scan();
    }

}
