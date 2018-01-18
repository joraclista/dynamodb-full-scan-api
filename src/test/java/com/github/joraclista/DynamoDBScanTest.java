package com.github.joraclista;

import com.amazonaws.regions.Regions;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

/**
 * Created by Alisa
 * version 1.0.
 */
@Slf4j
//@RunWith(JUnitPlatform.class)
public class DynamoDBScanTest {

    private Regions region = Regions.US_EAST_1;


    @DisplayName("Full table scan")
    @Test
    public void tableScanTest() {
        log.info("start");
        new DynamoItemsImporter<>(
                region,
                "Products",
                100,
                50,
                item -> item,
                Product.class
        ).getProcessedTableData().forEach(item -> log.info("item: {}", item));
    }


}
