package com.olah.gcloud.backup.server.dao;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import com.olah.gcloud.backup.api.model.Photo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PhotoDao {
    static {
        final InputStream inputStream = PhotoDao.class.getResourceAsStream("/logging.properties");
        try {
            java.util.logging.LogManager.getLogManager().readConfiguration(inputStream);
        } catch (final IOException e) {
            java.util.logging.Logger.getAnonymousLogger().severe("Could not load default logging.properties file");
            java.util.logging.Logger.getAnonymousLogger().severe(e.getMessage());
        }
    }

    static final Logger LOGGER = LogManager.getLogger(PhotoDao.class);

    public static final String FILE_NAME = "FILE_NAME";
    public static final String FOLDER_PATH = "FOLDER_PATH";
    public static final String STATUS = "STATUS_ATTRIBUTE";

    public static final String FOLDER_STATUS_INDEX = "FOLDER_STATUS_INDEX";

    private final String tableName;
    private AmazonDynamoDB client;
    private final DynamoDB dynamoDB;

    public PhotoDao(AmazonDynamoDB client, String tableName) {
        this.client = client;
        this.tableName = tableName;
        dynamoDB = new DynamoDB(client);
    }


    public void createTableIfNotExist() {
        CreateTableRequest request = new CreateTableRequest();

        request.setTableName(tableName);
        request.withKeySchema(
                new KeySchemaElement(FOLDER_PATH, KeyType.HASH),
                new KeySchemaElement(FILE_NAME, KeyType.RANGE)
                );
        request.withAttributeDefinitions(
                new AttributeDefinition(FOLDER_PATH, ScalarAttributeType.S),
                new AttributeDefinition(FILE_NAME, ScalarAttributeType.S),
                new AttributeDefinition(STATUS, ScalarAttributeType.S)
                );
        request.withProvisionedThroughput(
                new ProvisionedThroughput()
                        .withReadCapacityUnits(3L)
                        .withWriteCapacityUnits(3L)
        );

        GlobalSecondaryIndex globalSecondaryIndex = new GlobalSecondaryIndex();
        globalSecondaryIndex.setIndexName(FOLDER_STATUS_INDEX);
        globalSecondaryIndex.withKeySchema(
                new KeySchemaElement(FOLDER_PATH, KeyType.HASH), // partition
                new KeySchemaElement(STATUS, KeyType.RANGE)); //sort
        globalSecondaryIndex.withProjection(new Projection().withProjectionType(ProjectionType.ALL));
        globalSecondaryIndex.withProvisionedThroughput(new ProvisionedThroughput()
                .withReadCapacityUnits(1L)
                .withWriteCapacityUnits(1L));
        request.withGlobalSecondaryIndexes(globalSecondaryIndex);
        System.out.println("Attempting to create table; please wait...");

        dynamoDB.createTable(request);
        try {
            dynamoDB.getTable(tableName).waitForActive();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    public void storeOrUpdatePhoto(Photo photo) {
        System.out.println("Storing photo...");
        System.out.println("Photo details: " + photo);

        long start = System.currentTimeMillis();

        Map<String, AttributeValue> items = new HashMap<>();

        items.put(FILE_NAME, new AttributeValue().withS(photo.getFileName()));
        items.put(FOLDER_PATH, new AttributeValue().withS(photo.getFolderPath()));
        items.put(STATUS, new AttributeValue().withS(photo.getStatus()));

        PutItemRequest putItemRequest = new PutItemRequest(tableName, items);
        client.putItem(putItemRequest);

        System.out.println("Storing photo took:" + String.valueOf(System.currentTimeMillis() - start));
    }

    public Photo getPhoto(String folderPath, String fileName) {
        long start = System.currentTimeMillis();

        Map<String, AttributeValue> keys = new HashMap<>();
        keys.put(FILE_NAME, new AttributeValue().withS(fileName));
        keys.put(FOLDER_PATH, new AttributeValue().withS(folderPath));

        GetItemResult getItemResult = client.getItem(tableName, keys);

        Map<String, AttributeValue> item = getItemResult.getItem();
        if (item == null) {
            throw new IllegalArgumentException("Could not found photo with key " + folderPath + "." + fileName);
        }

        Photo result = new Photo();

        result.setFileName(item.get(FILE_NAME).getS());
        result.setFolderPath(item.get(FOLDER_PATH).getS());
        result.setStatus(Photo.StatusEnum.fromValue(item.get(STATUS).getS()));

        System.out.println("Get photo took:" + String.valueOf(System.currentTimeMillis() - start));

        return result;
    }


    public Set<Photo> getPhotosByFolder(String folderPath) {
        long start = System.currentTimeMillis();

        Table table = dynamoDB.getTable(tableName);
        Index index = table.getIndex(FOLDER_STATUS_INDEX);

        QuerySpec spec = new QuerySpec()
                .withKeyConditionExpression(FOLDER_PATH +" = :v_folder_path ")
                .withValueMap(new ValueMap()
                        .withString(":v_folder_path", folderPath));

        ItemCollection<QueryOutcome> items = index.query(spec);
        Stream<Item> targetStream = itemCollectiontoStream(items);
        Set<Photo> result = mapStreamToSet(targetStream);

        System.out.println("getPhotosByFolder took :" + String.valueOf(System.currentTimeMillis() - start));

        return result;
    }

    public Set<Photo> getPhotosByFolderAndStatus(String folderPath, Photo.StatusEnum status) {
        long start = System.currentTimeMillis();

        Table table = dynamoDB.getTable(tableName);
        Index index = table.getIndex(FOLDER_STATUS_INDEX);

        QuerySpec spec = new QuerySpec()
                .withKeyConditionExpression(FOLDER_PATH +" = :v_folder_path and " + STATUS + " = :v_status")
                .withValueMap(new ValueMap()
                        .withString(":v_folder_path", folderPath)
                        .withString(":v_status", status.value()));

        ItemCollection<QueryOutcome> items = index.query(spec);
        Stream<Item> targetStream = itemCollectiontoStream(items);
        Set<Photo> result = mapStreamToSet(targetStream);

        System.out.println("getPhotosByFolderAndStatus took :" + String.valueOf(System.currentTimeMillis() - start));

        return result;
    }

    private Set<Photo> mapStreamToSet(Stream<Item> targetStream) {
        return targetStream.map(item -> {
            Photo photo = new Photo();
            photo.setFileName(item.getString(FILE_NAME));
            photo.setFolderPath(item.getString(FOLDER_PATH));
            photo.setStatus(Photo.StatusEnum.fromValue(item.getString(STATUS)));
            return photo;
        }).collect(Collectors.toSet());
    }

    private Stream<Item> itemCollectiontoStream(ItemCollection<QueryOutcome> items) {
        Iterator<Item> iterator = items.iterator();

        Iterable<Item> iterable = () -> iterator;

        return StreamSupport.stream(iterable.spliterator(), false);
    }


    // only for testing
    void deleteTable() {
        System.out.println("Deleting table " + tableName + "...");

        boolean tableAlreadyExists =
        StreamSupport.stream(dynamoDB.listTables().spliterator(), false).anyMatch(table -> table.getTableName().equals(tableName));

        if(!tableAlreadyExists) {
            return;
        }

        Table table = dynamoDB.getTable(tableName);

        table.delete();

        // Wait for table to be deleted
        System.out.println("Waiting for " + tableName + " to be deleted...");
        try {
            table.waitForDelete();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}