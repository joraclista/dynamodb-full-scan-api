package com.github.joraclista.scanner;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.github.joraclista.api.DynamoDbAPI;
import com.github.joraclista.scanner.strategies.BaseScanStrategy;
import com.github.joraclista.scanner.strategies.ConsumeBatchScanStrategy;
import com.github.joraclista.scanner.strategies.ConsumeOneByOneScanStrategy;
import com.github.joraclista.scanner.strategies.ReadScanStrategy;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.function.Consumer;

/**
 * Created by Alisa
 * version 1.0.
 */
@Slf4j
public class DynamoItemsImporter<T> {

    private static final int ITEMS_PER_DYNAMO_SCAN = 200;
    private static final int PAUSE_BETWEEN_DYNAMO_SCANS_IN_MS = 50;

    private final DynamoDbAPI dynamoClient;
    private final Class<T> tableMappingClass;

    private int itemsPerScan = ITEMS_PER_DYNAMO_SCAN;
    private int pauseBetweenScans = PAUSE_BETWEEN_DYNAMO_SCANS_IN_MS;


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

    /**
     * Sets pause between table reads in milliseconds
     * if null, then default (PAUSE_BETWEEN_DYNAMO_SCANS_IN_MS) is used
     * @param pauseBetweenScans in milliseconds
     * @return
     */
    public DynamoItemsImporter<T> withPauseBetweenScans(Integer pauseBetweenScans) {
        this.pauseBetweenScans = pauseBetweenScans == null ? PAUSE_BETWEEN_DYNAMO_SCANS_IN_MS : pauseBetweenScans;
        return this;
    }

    /**
     * Sets number of items to read during each table read
     * if null, then default (ITEMS_PER_DYNAMO_SCAN) is used
     * @param itemsPerScan
     * @return
     */
    public DynamoItemsImporter<T> withItemsPerScan(Integer itemsPerScan) {
        this.itemsPerScan = itemsPerScan == null ? ITEMS_PER_DYNAMO_SCAN : itemsPerScan;
        return this;
    }

    /**
     * Performs sequential dynamoDB table reads with
     * {@code pauseBetweenScans} ms pause between reads
     * {@code itemsPerScan} items per each read
     */
    public List<T> getTableData() {
       return scan(ReadScanStrategy.<T>builder()
                .dynamoClient(dynamoClient)
                .tableMappingClass(tableMappingClass)
                .itemsPerScan(itemsPerScan)
                .pauseBetweenScans(pauseBetweenScans)
                .build());
    }

    /**
     * Performs dynamoDB table reads with
     * {@code pauseBetweenScans} ms pause between reads
     * {@code itemsPerScan} items per each read
     * @param itemsConsumer - consumer to perform desired operations on each item
     */
    public void consumeTableData(Consumer<T> itemsConsumer) {
        scan(ConsumeOneByOneScanStrategy.<T>builder()
                .dynamoClient(dynamoClient)
                .tableMappingClass(tableMappingClass)
                .itemsPerScan(itemsPerScan)
                .pauseBetweenScans(pauseBetweenScans)
                .itemsConsumer(itemsConsumer)
                .build());
    }

    /**
     * Performs dynamoDB table reads with
     * {@code pauseBetweenScans} ms pause between reads
     * {@code itemsPerScan} items per each read
     * @param itemsBatchConsumer - consumer to perform desired operations on {@code itemsPerScan}-items batch
     */
    public void consumeBatchTableData(Consumer<List<T>> itemsBatchConsumer) {
        scan(ConsumeBatchScanStrategy.<T>builder()
                .dynamoClient(dynamoClient)
                .tableMappingClass(tableMappingClass)
                .itemsPerScan(itemsPerScan)
                .pauseBetweenScans(pauseBetweenScans)
                .itemsBatchConsumer(itemsBatchConsumer)
                .build());
    }

    @SneakyThrows(InterruptedException.class)
    private <R> R scan(BaseScanStrategy<T, R> scanStrategy) {
        return scanStrategy.scan();
    }

}
