package com.github.joraclista.model;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import lombok.Data;

/**
 * Created by Alisa
 * version 1.0.
 */
@DynamoDBTable(tableName = "Products")
@Data
public class NotAnnotatedHashKeyModel {
    private String id;
}
