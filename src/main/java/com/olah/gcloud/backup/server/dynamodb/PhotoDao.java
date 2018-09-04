package com.olah.gcloud.backup.server.dynamodb;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.olah.gcloud.backup.api.model.Photo;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PhotoDao {

    public static final String PHOTO_TABLE_NAME = "DeviceStatus";

    public static final String FILE_NAME = "FILE_NAME";
    public static final String FOLDER_PATH = "FOLDER_PATH";
    public static final String STATUS = "STATUS";


    public static final String FOLDER_NAME_WITH_FILE_NAME = "FOLDER_NAME_WITH_FILE_NAME";
    public static final String FOLDER_NAME_WITH_STATUS = "FOLDER_NAME_WITH_STATUS";

    private AmazonDynamoDB client;

    /**
     * @param endPointUrl e.g.: http://localhost:8000
     */
    public PhotoDao(String endPointUrl) {
        if (endPointUrl == null) {
            throw new IllegalArgumentException("endpoint parameter cannot be null");
        }

        client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endPointUrl, "us-west-2"))
                .withCredentials(new ProfileCredentialsProvider("private"))
                .build();
    }


    public void createTable(String tableName) {
        CreateTableRequest request = new CreateTableRequest();

        request.setTableName(tableName);
        request.withKeySchema(new KeySchemaElement(FOLDER_NAME_WITH_FILE_NAME, KeyType.HASH));
        request.withAttributeDefinitions(
                new AttributeDefinition(FOLDER_NAME_WITH_FILE_NAME, ScalarAttributeType.S),
                new AttributeDefinition(FOLDER_NAME_WITH_STATUS, ScalarAttributeType.S)

        );

        request.withProvisionedThroughput(
                new ProvisionedThroughput()
                        .withReadCapacityUnits(3L)
                        .withWriteCapacityUnits(3L)
        );


        GlobalSecondaryIndex globalSecondaryIndex = new GlobalSecondaryIndex();
        globalSecondaryIndex.setIndexName("FOLDER_STATUS_INDEX");
        globalSecondaryIndex.withKeySchema(new KeySchemaElement(FOLDER_NAME_WITH_STATUS, KeyType.HASH));
        globalSecondaryIndex.withProjection(new Projection().withProjectionType(ProjectionType.ALL));
        globalSecondaryIndex.withProvisionedThroughput(                new ProvisionedThroughput()
                .withReadCapacityUnits(1L)
                .withWriteCapacityUnits(1L));
        request.setGlobalSecondaryIndexes(Arrays.asList(globalSecondaryIndex));
        System.out.println("Attempting to create table; please wait...");

        TableUtils.createTableIfNotExists(client, request);
        try {
            TableUtils.waitUntilActive(client, PHOTO_TABLE_NAME);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    public void storeOrUpdatePhoto(Photo photo) {
        Map<String, AttributeValue> items = new HashMap<>();
        items.put(FOLDER_NAME_WITH_FILE_NAME, new AttributeValue().withS(createFilePath(photo.getFolderPath(), photo.getFileName()))); // key
        items.put(FOLDER_NAME_WITH_STATUS, new AttributeValue().withS(createFolderWithStatus(photo.getFolderPath(), photo.getStatus()))); // index key

        items.put(FILE_NAME, new AttributeValue().withS(photo.getFileName()));
        items.put(FOLDER_PATH, new AttributeValue().withS(photo.getFolderPath()));
        items.put(STATUS, new AttributeValue().withS(photo.getStatus()));

        PutItemRequest putItemRequest = new PutItemRequest(PHOTO_TABLE_NAME, items);
        client.putItem(putItemRequest);
    }

    private String createFolderWithStatus(String folderPath, String status) {
        return folderPath + "_" + status;
    }

    private String createFilePath(String folderPath, String filename) {
        return folderPath + "/" + filename;
    }

    public Photo getPhoto(String folderPath, String fileName) {
        String key = createFilePath(folderPath, fileName);


        Map<String, AttributeValue> keys = new HashMap<>();
        keys.put(FOLDER_NAME_WITH_FILE_NAME, new AttributeValue().withS(key));
        GetItemResult getItemResult = client.getItem(PHOTO_TABLE_NAME, keys);

        if (getItemResult.getItem() == null) {
            throw new IllegalArgumentException("Could not found photo with key " + key);
        }

        Photo result = new Photo();

        result.setFileName(getItemResult.getItem().get(FILE_NAME).getS());
        result.setFolderPath(getItemResult.getItem().get(FOLDER_PATH).getS());
        result.setStatus(Photo.StatusEnum.valueOf(getItemResult.getItem().get(STATUS).getS()));

        return result;
    }
}