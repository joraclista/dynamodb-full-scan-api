package com.github.joraclista.scanner;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMappingException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.github.joraclista.model.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

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
        new DynamoItemsImporter<>(region, Product.class)
                .withItemsMappingFunction(item -> item)
                .withItemsPerScan(100)
                .withPauseBetweenScans(50)
                .getTableData()
                .forEach(item -> log.info("item: {}", item));
    }

    @DisplayName("Not Annotated Mapping Class Test")
    @Test
    public void tableScanTestNotAnnotatedMappingClass() {
        Throwable exception = assertThrows(RuntimeException.class, () -> new DynamoItemsImporter<>(region, NotAnnotatedModel.class)
                .withItemsMappingFunction(item -> item)
                .withItemsPerScan(100)
                .getTableData());
        assertEquals(exception.getMessage(), "Mapping class '" + NotAnnotatedModel.class.getName() + "' should be annotated with @DynamoDBTable annotation");
    }

    @DisplayName("Not Annotated Hash Key")
    @Test
    public void tableScanTestNotAnnotatedHashKey() {
        Throwable exception = assertThrows(DynamoDBMappingException.class, () -> new DynamoItemsImporter<>(region, NotAnnotatedHashKeyModel.class)
                .withItemsMappingFunction(item -> item)
                .withItemsPerScan(100)
                .getTableData());
        assertTrue(exception.getMessage().contains("no mapping for HASH key"));
    }

    @DisplayName("Test table name")
    @Test
    public void tableNameEmptyOrNullTest() {
        Throwable exception = assertThrows(IllegalArgumentException.class, () -> new DynamoItemsImporter<>(region, EmptyTableName.class)
                .withItemsMappingFunction(item -> item)
                .withItemsPerScan(100)
                .getTableData());
        assertEquals(exception.getMessage(), "@DynamoDBTable annotation should have valid tableName");
    }

    @DisplayName("Test table name")
    @Test
    public void tableNameDoesNotExistTest() {
        Throwable exception = assertThrows(ResourceNotFoundException.class, () -> new DynamoItemsImporter<>(region, NoSuchTable.class)
                .withItemsMappingFunction(item -> item)
                .withItemsPerScan(100)
                .getTableData());
        assertTrue(exception.getMessage().contains("Requested resource not found"));
    }

    @DisplayName("Consume one by one 'Order' table items")
    @Test
    public void orderTableConsumeOperationTest() {
        new DynamoItemsImporter<>(region, Order.class)
                .consumeTableData(item -> log.info("item: {}", item));
    }

    @DisplayName("Map + Consume one by one 'Order' table items")
    @Test
    public void orderTableMapAndConsumeOperationTest() {
        new DynamoItemsImporter<Order, Order>(region, Order.class)
                .withItemsMappingFunction(item -> {item.setId("***" + item.getId() + "***"); return item;})
                .consumeTableData(item -> log.info("item: {}", item));
    }
}
