package com.github.joraclista.scanner;

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
        new DynamoItemsImporter<>(region, "Products", Product.class)
                .withItemsMappingFunction(item -> item)
                .withItemsPerScan(100)
                .withPauseBetweenScans(50)
                .getTableData()
                .forEach(item -> log.info("item: {}", item));
    }

    @DisplayName("Not Annotated Mapping Class Test")
    @Test
    public void tableScanTestNotAnnotatedMappingClass() {
        Throwable exception = assertThrows(RuntimeException.class, () -> new DynamoItemsImporter<>(region, "Products", NotAnnotatedModel.class)
                .withItemsMappingFunction(item -> item)
                .withItemsPerScan(100)
                .getTableData()
                .forEach(item -> log.info("item: {}", item)));
        assertEquals(exception.getMessage(), "Mapping class '" + NotAnnotatedModel.class.getName() + "' should be annotated with @DynamoDBTable annotation");
    }

    @DisplayName("Not Annotated Hash Key")
    @Test
    public void tableScanTestNotAnnotatedHashKey() {
        Throwable exception = assertThrows(DynamoDBMappingException.class, () -> new DynamoItemsImporter<>(region, "Products", NotAnnotatedHashKeyModel.class)
                .withItemsMappingFunction(item -> item)
                .withItemsPerScan(100)
                .getTableData()
                .forEach(item -> log.info("item: {}", item)));
        assertTrue(exception.getMessage().contains("no mapping for HASH key"));
    }

    @DisplayName("Test table name")
    @Test
    public void tableNameEmptyOrNullTest() {
        Throwable exception = assertThrows(IllegalArgumentException.class, () -> new DynamoItemsImporter<>(region, "Orders", Order.class)
                .withItemsMappingFunction(item -> item)
                .withItemsPerScan(100)
                .getTableData()
                .forEach(item -> log.info("item: {}", item)));
        assertTrue(exception.getMessage().contains("Table name should not be empty or null"));
    }

    @DisplayName("Consume one by one 'Order' table items")
    @Test
    public void orderTableConsumeOperationTest() {
        new DynamoItemsImporter<>(region, "Orders", Order.class)
                .consumeTableData(item -> log.info("item: {}", item));
    }

    @DisplayName("Map + Consume one by one 'Order' table items")
    @Test
    public void orderTableMapAndConsumeOperationTest() {
        new DynamoItemsImporter<Order, Order>(region, "Orders", Order.class)
                .withItemsMappingFunction(item -> {item.setId("***" + item.getId() + "***"); return item;})
                .consumeTableData(item -> log.info("item: {}", item));
    }
}
