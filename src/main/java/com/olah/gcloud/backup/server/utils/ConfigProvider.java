package com.olah.gcloud.backup.server.utils;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;

public class ConfigProvider {
    public static final String DYNAMODB_END_POINT_KEY = "DYNAMODB_END_POINT_KEY";
    public static final String DYNAMODB_SIGNING_REGION_KEY = "DYNAMODB_SIGNING_REGION_KEY";

    public static Environment getEnvironment() {
        String environmentProvided = System.getenv("ENVIRONMENT");

        if(environmentProvided == null) {
            System.out.println("No environment provided, using local");

            return Environment.LOCAL;
        }

        return Environment.valueOf(environmentProvided);
    }

    public static AmazonDynamoDB getAmazonDynamoDB() {
        String endPointUrl;
        String signingRegion;

        if(getEnvironment().equals(Environment.LOCAL)) {
            endPointUrl = "http://localhost:9000";
            signingRegion = "";
        }
        else {
            endPointUrl = getEnvironmentVariable(DYNAMODB_END_POINT_KEY);// dynamodb.eu-central-1.amazonaws.com
            signingRegion = getEnvironmentVariable(DYNAMODB_SIGNING_REGION_KEY); // eu-central-1
        }

        return AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endPointUrl, signingRegion))
                .withCredentials(new ProfileCredentialsProvider("private"))
                .build();
    }

    private static String getEnvironmentVariable(String key) {
        return System.getenv(key);
    }

    public static String getTableName() {
        switch (getEnvironment()) {
            case PROD:
                return "DeviceStatus";
            case LOCAL:
                return "DeviceStatus";
            case TEST_ON_AWS:
                return "DeviceStatusTest";
        }

        throw new IllegalStateException("invalid environment");
    }

    public static enum Environment {
        PROD, TEST_ON_AWS, LOCAL
    }
}
