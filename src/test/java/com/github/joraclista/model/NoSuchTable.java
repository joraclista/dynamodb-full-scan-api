package com.github.joraclista.model;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import lombok.Data;

/**
 * Created by Alisa
 * version 1.0.
 */
@DynamoDBTable(tableName = "SomeTable")
@Data
public class NoSuchTable {

    @DynamoDBHashKey
    private String id;

}