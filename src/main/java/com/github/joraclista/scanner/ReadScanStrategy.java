package com.github.joraclista.scanner;

import com.github.joraclista.api.DynamoDbAPI;
import com.github.joraclista.scanner.strategies.BaseScanStrategy;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Created by Alisa
 * version 1.0.
 */
@Slf4j
public class ReadScanStrategy<T, E> extends BaseScanStrategy<T, E, List<E>> {

    @Builder
    public ReadScanStrategy(DynamoDbAPI dynamoClient,
                            Class<T> tableMappingClass,
                            int itemsPerScan,
                            int pauseBetweenScans,
                            Function<T, E> itemsMappingFunction) {
        super(dynamoClient, tableMappingClass, itemsPerScan, pauseBetweenScans, itemsMappingFunction);
    }

    protected void validateArgs() {
        requireNonNull(itemsMappingFunction, "Items mapping function shouldn't be null");
    }

    @Override
    @SneakyThrows(InterruptedException.class)
    public List<E> scan() {
        val result = new ArrayList<E>();
        validateArgs();
        log.info("scan: received items from dynamoDb");

        val iterator = getPaginatedList().iterator();
        int counter = 0;

        while (iterator.hasNext()) {
            val item = iterator.next();
            try {
                E mappedItem = itemsMappingFunction.apply(item);
                result.add(mappedItem);

            } catch (Exception e) {
                log.error("scan: Couldn't process item : due to {}", e.getMessage());
            }

            if (counter % (itemsPerScan - 1) == 0 && counter > 0 || !iterator.hasNext()) {
                log.info("scan: We've reached the threshold [{} items], going to sleep for {} ms.", itemsPerScan, pauseBetweenScans);
                Thread.sleep(pauseBetweenScans);
            }
            counter++;
        }
        return result;
    }
}
