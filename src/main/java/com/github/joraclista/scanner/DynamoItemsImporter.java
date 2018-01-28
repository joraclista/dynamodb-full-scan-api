package com.github.joraclista.scanner;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedScanList;
import com.github.joraclista.api.DynamoDbAPI;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
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

    private enum Operation {READ_ALL, CONSUME_ONE_BY_ONE}

    private final DynamoDbAPI dynamoClient;
    private final String tableName;
    private final Class<T> tableMappingClass;

    private int itemsPerScan;
    private int pauseBetweenScans;
    private Function<T, E> itemsMappingFunction;


    public DynamoItemsImporter(Regions regions,
                               String tableName,
                               Class<T> tableMappingClass) {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name should not be empty or null");
        }
        if (tableMappingClass.getAnnotationsByType(DynamoDBTable.class).length == 0) {
            throw new IllegalArgumentException("Mapping class '" + tableMappingClass.getName() + "' should be annotated with @DynamoDBTable annotation");
        }
        this.dynamoClient = new DynamoDbAPI(regions);
        this.tableName = tableName;


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

    private PaginatedScanList<T> getPaginatedList() {
        DynamoDBScanExpression scanExpression = new DynamoDBScanExpression()
                .withLimit(itemsPerScan);

        return dynamoClient.getMapper().scan(tableMappingClass, scanExpression, new DynamoDBMapperConfig.Builder().withPaginationLoadingStrategy(LAZY_LOADING)
                .build());
    }

    public List<E> getTableData() {
        return doOperation(Operation.READ_ALL, null);
    }

    public void consumeTableData(Consumer<E> itemsConsumer) {
        doOperation(Operation.CONSUME_ONE_BY_ONE, itemsConsumer);
    }


    @SneakyThrows(InterruptedException.class)
    private List<E> doOperation(Operation operation, Consumer<E> itemsConsumer) {
        val result = new ArrayList<E>();
        requireNonNull(itemsMappingFunction, "Items mapping function shouldn't be null");
        if (Operation.CONSUME_ONE_BY_ONE.equals(operation))
            requireNonNull(itemsConsumer, "Items consumer function shouldn't be null");
        log.info("doOperation: starting scan for table = '{}'", tableName);

        val list = getPaginatedList();
        log.info("doOperation: received items from dynamoDb");

        val iterator = list.iterator();
        int counter = 0;

        while (iterator.hasNext()) {
            val item = iterator.next();
            try {
                E mappedItem = itemsMappingFunction.apply(item);
                switch (operation) {
                    case CONSUME_ONE_BY_ONE:
                        itemsConsumer.accept(mappedItem);
                        break;
                    case READ_ALL:
                        result.add(mappedItem);
                        break;
                }

            } catch (Exception e) {
                log.error("doOperation: Couldn't process item : due to {}", e.getMessage());
            }

            if (counter % (itemsPerScan - 1) == 0 && counter > 0 || !iterator.hasNext()) {
                log.info("doOperation: We've reached the threshold [{} items], going to sleep for {} ms.", itemsPerScan, pauseBetweenScans);
                Thread.sleep(pauseBetweenScans);
            }
            counter++;
        }
        return result;
    }
}
