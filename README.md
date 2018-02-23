# dynamodb-full-scan-api
Full scan for dynamodb table

### Prerequisites

This api works with AWS DynamoDB thus you should have
1)  AWS account set up and configured
2) *.aws/credentials* file in your file system OR env variables for access key/secret configured.
  Pls refer to [documentation](https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html)
  #### Explanation: 
  Tests use DefaultAWSCredentialsProviderChain which looks for credentials in this order:
  
  * Environment Variables - *AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY* (recognized by all the AWS SDKs and CLI),
    or *AWS_ACCESS_KEY / AWS_SECRET_KEY* (only recognized by Java SDK)
  * Java System Properties - *aws.accessKeyId* and *aws.secretKey*
  * Credential profiles file at the default location (*~/.aws/credentials*)
3) dynamoDB storage service activated for your account
4) for test purposes pls [create table](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SampleData.CreateTables.html) ShopProducts with simple key id (string)

### Build & Test
This is a regular, Maven based project.

Just run `mvn clean package`

### Usage

### Get list of all items in dynamodb table:
##### 1. Create Mapped Class for your source table using [DynamoDBTable annotation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBMapper.Annotations.html#DynamoDBMapper.Annotations.DynamoDBTable) and [DynamoDBHashKey annotation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBMapper.Annotations.html#DynamoDBMapper.Annotations.DynamoDBHashKey)

For instance next mapped class corresponds to dynamodb *"Products"* table with *"id"* as a hash key:
  ```java
@DynamoDBTable(tableName = "Products")
public class Product {

    @DynamoDBHashKey
    private String id;
    private String merchantId;
    private String title;
    private String currency;
    private double price;
    //other stufff.......
    //getters/setters/etc
}
```
##### 2.  Create DynamoItemsImporter instance for desired aws region and mapped class.
```java
new DynamoItemsImporter<>(Regions.US_EAST_1, Product.class)
                .withItemsPerScan(100)
                .withPauseBetweenScans(50)
                .getTableData()
                .forEach(item -> log.info("item: {}", item))
```
Optionally pass ItemsPerScan/PauseBetweenScans values, otherwise default will be used (itemsPerScan=100 and pauseBetweenScans=50ms)

### Consume one by one all items in dynamodb table:
1. Create Mapped Class for your source table [see above](https://github.com/joraclista/dynamodb-full-scan-api/blob/master/README.md#1-create-mapped-class-for-your-source-table-using-dynamodbtable-annotation-and-dynamodbhashkey-annotation)
2.  Create DynamoItemsImporter instance for desired aws region and mapped class.
```java
new DynamoItemsImporter<>(Regions.US_EAST_1, Product.class)
                .withItemsPerScan(100)
                .withPauseBetweenScans(50)
                .consumeTableData(item -> log.info("item [id:title] = [{}:{}]", item.getId(), item.getTitle()));
```
Optionally pass ItemsPerScan/PauseBetweenScans values, otherwise default will be used (itemsPerScan=100 and pauseBetweenScans=50ms)

### Batch Consume all items in dynamodb table with batch size=itemsPerScan:
1.  Create Mapped Class for your source table [see above](https://github.com/joraclista/dynamodb-full-scan-api/blob/master/README.md#1-create-mapped-class-for-your-source-table-using-dynamodbtable-annotation-and-dynamodbhashkey-annotation)
2.  Create DynamoItemsImporter instance for desired aws region and mapped class.
```java
new DynamoItemsImporter<>(Regions.US_EAST_1, Product.class)
                .consumeBatchTableData(list -> log.info("count: {}", list.size()));
```
Optionally pass ItemsPerScan/PauseBetweenScans values, otherwise default will be used (itemsPerScan=100 and pauseBetweenScans=50ms)
