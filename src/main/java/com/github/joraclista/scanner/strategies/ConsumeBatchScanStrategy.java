package com.github.joraclista.scanner.strategies;

import com.github.joraclista.api.DynamoDbAPI;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Created by Alisa
 * version 1.0.
 */
@Slf4j
public class ConsumeBatchScanStrategy<T> extends BaseScanStrategy<T, Void> {

    @Builder
    public ConsumeBatchScanStrategy(DynamoDbAPI dynamoClient,
                                    Class<T> tableMappingClass,
                                    int itemsPerScan,
                                    int pauseBetweenScans,
                                    Consumer<List<T>> itemsBatchConsumer) {
        super(dynamoClient, tableMappingClass, itemsPerScan, pauseBetweenScans);
        this.itemsBatchConsumer = itemsBatchConsumer;
    }

    protected void validateArgs() {
        requireNonNull(itemsBatchConsumer, "Items Batch consumer shouldn't be null");
    }

    @Override
    @SneakyThrows(InterruptedException.class)
    public Void scan() {
        val batch = new ArrayList<T>();
        validateArgs();
        log.info("scan: received items from dynamoDb");

        val iterator = getPaginatedList().iterator();
        int counter = 0;

        while (iterator.hasNext()) {
            batch.add(iterator.next());

            if (++counter % itemsPerScan == 0 && counter > 0 || !iterator.hasNext()) {
                log.info("scan: We've reached the threshold [{} items], going to sleep for {} ms.", itemsPerScan, pauseBetweenScans);
                itemsBatchConsumer.accept(batch);
                batch.clear();
                Thread.sleep(pauseBetweenScans);
            }
        }
        return null;
    }
}
