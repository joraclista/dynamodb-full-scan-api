package com.github.joraclista;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMappingException;
import com.github.joraclista.model.NotAnnotatedHashKeyModel;
import com.github.joraclista.model.NotAnnotatedModel;
import com.github.joraclista.model.Order;
import com.github.joraclista.model.Product;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Created by Alisa
 * version 1.0.
 */
@Slf4j
public class DynamoDBScanTest {

    private Regions region = Regions.US_EAST_1;


    @DisplayName("Full 'Products' table scan")
    @Test
    public void productsTableScanTest() {
        new DynamoItemsImporter<>(
                region,
                "Products",
                100,
                50,
                item -> item,
                Product.class
        ).getProcessedTableData().forEach(item -> log.info("item: {}", item));
    }

    @DisplayName("Not Annotated Mapping Class Test")
    @Test
    public void tableScanTestNotAnnotatedMappingClass() {
        Throwable exception = assertThrows(RuntimeException.class, () -> new DynamoItemsImporter<>(
                region,
                "Products",
                100,
                50,
                item -> item,
                NotAnnotatedModel.class
        ).getProcessedTableData().forEach(item -> log.info("item: {}", item)));
        assertEquals(exception.getMessage(), "Mapping class '" + NotAnnotatedModel.class.getName() + "' should be annotated with @DynamoDBTable annotation");
    }

    @DisplayName("Not Annotated Hash Key")
    @Test
    public void tableScanTestNotAnnotatedHashKey() {
        Throwable exception = assertThrows(DynamoDBMappingException.class, () -> new DynamoItemsImporter<>(
                region,
                "Products",
                100,
                50,
                item -> item,
                NotAnnotatedHashKeyModel.class
        ).getProcessedTableData().forEach(item -> log.info("item: {}", item)));
        assertTrue(exception.getMessage().contains("no mapping for HASH key"));
    }

    @DisplayName("Full 'Order' table scan")
    @Test
    public void orderTableScanTest() {
        new DynamoItemsImporter<>(
                region,
                "Orders",
                100,
                50,
                item -> item,
                Order.class
        ).getProcessedTableData().forEach(item -> log.info("item: {}", item));
    }
}
