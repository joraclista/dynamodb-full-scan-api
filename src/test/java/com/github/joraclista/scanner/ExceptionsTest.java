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
public class ExceptionsTest {

    private Regions region = Regions.US_EAST_1;

    @DisplayName("Not Annotated Mapping Class Test")
    @Test
    public void tableScanTestNotAnnotatedMappingClass() {
        Throwable exception = assertThrows(RuntimeException.class, () -> new DynamoItemsImporter<>(region, NotAnnotatedModel.class)
                .withItemsPerScan(100)
                .getTableData());
        assertEquals(exception.getMessage(), "Mapping class '" + NotAnnotatedModel.class.getName() + "' should be annotated with @DynamoDBTable annotation");
    }

    @DisplayName("Not Annotated Hash Key Test")
    @Test
    public void tableScanTestNotAnnotatedHashKey() {
        Throwable exception = assertThrows(DynamoDBMappingException.class, () -> new DynamoItemsImporter<>(region, NotAnnotatedHashKeyModel.class)
                .withItemsPerScan(100)
                .getTableData());
        assertTrue(exception.getMessage().contains("no mapping for HASH key"));
    }

    @DisplayName("Invalid Table Name Test")
    @Test
    public void tableNameEmptyOrNullTest() {
        Throwable exception = assertThrows(IllegalArgumentException.class, () -> new DynamoItemsImporter<>(region, EmptyTableName.class)
                .withItemsPerScan(100)
                .getTableData());
        assertEquals(exception.getMessage(), "@DynamoDBTable annotation should have valid tableName");
    }

    @DisplayName("Table Resource Not Found Test")
    @Test
    public void tableNameDoesNotExistTest() {
        Throwable exception = assertThrows(ResourceNotFoundException.class, () -> new DynamoItemsImporter<>(region, NoSuchTable.class)
                .withItemsPerScan(100)
                .getTableData());
        assertTrue(exception.getMessage().contains("Requested resource not found"));
    }

}
