package com.github.joraclista.scanner;

import com.amazonaws.regions.Regions;
import com.github.joraclista.model.Order;
import com.github.joraclista.model.Product;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

/**
 * Created by Alisa
 * version 1.0.
 */
@Slf4j
public class DynamoDBScanTest {

    private Regions region = Regions.US_EAST_1;

    @DisplayName("'Products' Table Full Scan Test")
    @Test
    public void productsTableScanTest() {
        new DynamoItemsImporter<>(region, Product.class)
                .withItemsPerScan(100)
                .withPauseBetweenScans(50)
                .getTableData()
                .forEach(item -> log.info("item: {}", item));
    }

    @DisplayName("'Products' Table Batch Consume Test")
    @Test
    public void productsTableBatchConsumeTest() {
        new DynamoItemsImporter<>(region, Product.class)
                .withItemsPerScan(100)
                .withPauseBetweenScans(50)
                .consumeBatchTableData(list -> log.info("count: {}", list.size()));
    }

    @DisplayName("'Products' Table Consume Test")
    @Test
    public void productsTableConsumeTest() {
        new DynamoItemsImporter<>(region, Product.class)
                .withItemsPerScan(100)
                .withPauseBetweenScans(50)
                .consumeTableData(item -> log.info("item [id:title] = [{}:{}]", item.getId(), item.getTitle()));
    }

    @DisplayName("Consume one by one 'Order' table items")
    @Test
    public void orderTableConsumeOperationTest() {
        new DynamoItemsImporter<>(region, Order.class)
                .consumeTableData(item -> log.info("item: {}", item));
    }

}
