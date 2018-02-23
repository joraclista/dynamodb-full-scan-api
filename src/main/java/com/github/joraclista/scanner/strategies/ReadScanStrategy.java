package com.github.joraclista.scanner.strategies;

import com.github.joraclista.api.DynamoDbAPI;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Alisa
 * version 1.0.
 */
@Slf4j
public class ReadScanStrategy<T> extends BaseScanStrategy<T, List<T>> {

    @Builder
    public ReadScanStrategy(DynamoDbAPI dynamoClient,
                            Class<T> tableMappingClass,
                            int itemsPerScan,
                            int pauseBetweenScans) {
        super(dynamoClient, tableMappingClass, itemsPerScan, pauseBetweenScans);
    }

    @Override
    @SneakyThrows(InterruptedException.class)
    public List<T> scan() {
        val result = new ArrayList<T>();
        log.info("scan: received items from dynamoDb");

        val iterator = getPaginatedList().iterator();
        int counter = 0;

        while (iterator.hasNext()) {
            result.add(iterator.next());

            if (++counter % itemsPerScan == 0 && counter > 0 || !iterator.hasNext()) {
                log.info("scan: We've reached the threshold [{} items], going to sleep for {} ms.", itemsPerScan, pauseBetweenScans);
                Thread.sleep(pauseBetweenScans);
            }
        }
        return result;
    }
}
