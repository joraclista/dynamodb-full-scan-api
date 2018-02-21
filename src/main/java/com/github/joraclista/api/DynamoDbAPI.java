package com.github.joraclista.api;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by Alisa
 * version 1.0.
 */
@Slf4j
public class DynamoDbAPI {
    private final AmazonDynamoDB amazonClient;
    @Getter
    private final DynamoDBMapper mapper;


    public DynamoDbAPI(Regions regions) {
        amazonClient = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .withRegion(regions)
                .build();
        mapper = new DynamoDBMapper(amazonClient);

    }

}


