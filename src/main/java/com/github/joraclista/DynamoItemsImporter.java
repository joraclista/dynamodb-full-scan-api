package com.github.joraclista;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedScanList;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.PaginationLoadingStrategy.LAZY_LOADING;
import static java.util.Objects.requireNonNull;

/**
 * Created by Alisa
 * version 1.0.
 */
@Slf4j
public class DynamoItemsImporter<T, E> {

    private static final int ITEMS_PER_DYNAMO_SCAN = 200;
    private static final int PAUSE_BETWEEN_DYNAMO_SCANS_IN_MS = 50;

    private final DynamoDbAPI dynamoClient;
    private final int itemsPerScan;
    private final int pauseBetweenScans;
    private final Function<T, E> itemsMappingFunction;
    private final String tableName;
    private final Class<T> tableMappingClass;

    public DynamoItemsImporter(Regions regions,
                               String tableName,
                               Integer itemsPerScan,
                               Integer pauseBetweenScans,
                               Function<T, E> itemsMappingFunction,
                               Class<T> tableMappingClass) {
        this.dynamoClient = new DynamoDbAPI(regions);
        this.tableName = tableName;
        this.itemsPerScan = itemsPerScan == null ? ITEMS_PER_DYNAMO_SCAN : itemsPerScan;
        this.pauseBetweenScans = pauseBetweenScans == null ? PAUSE_BETWEEN_DYNAMO_SCANS_IN_MS : pauseBetweenScans;
        this.itemsMappingFunction = itemsMappingFunction;
        if (tableMappingClass.getAnnotationsByType(DynamoDBTable.class).length == 0) {
            throw new IllegalArgumentException("Mapping class '" + tableMappingClass.getName() + "' should be annotated with @DynamoDBTable annotation");
        }
        this.tableMappingClass = tableMappingClass;
    }

    private PaginatedScanList<T> getPaginatedList() {
        DynamoDBScanExpression scanExpression = new DynamoDBScanExpression()
                .withLimit(itemsPerScan);

        return dynamoClient.getMapper().scan(tableMappingClass, scanExpression, new DynamoDBMapperConfig.Builder().withPaginationLoadingStrategy(LAZY_LOADING)
                .build());
    }

    @SneakyThrows(InterruptedException.class)
    public List<E> getProcessedTableData() {
        val result = new ArrayList<E>();
        requireNonNull(tableName, "Table Name shouldn't be null");
        requireNonNull(itemsMappingFunction, "Items mapping function shouldn't be null");

        log.info("processTableData: from table = {}", tableName);

        val list = getPaginatedList();
        log.info("updateData: received items from dynamoDb");

        val iterator = list.iterator();
        int counter = 0;

        val preparedItems = new ArrayList<E>();

        while (iterator.hasNext()) {
            val item = iterator.next();
            try {
                preparedItems.add(itemsMappingFunction.apply(item));
            } catch (Exception e) {
                log.error("Couldn't process item :  due to {}", e.getMessage());
            }

            if (counter % (itemsPerScan - 1) == 0 && counter > 0 || !iterator.hasNext()) {
                log.info("We've reached the threshold [{} items], going to sleep for {} ms.", itemsPerScan, pauseBetweenScans);
                result.addAll(preparedItems);
                preparedItems.clear();
                Thread.sleep(pauseBetweenScans);
            }
            counter++;
        }
        return result;
    }

}
