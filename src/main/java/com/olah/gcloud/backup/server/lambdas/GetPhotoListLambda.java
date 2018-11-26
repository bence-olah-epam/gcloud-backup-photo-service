package com.olah.gcloud.backup.server.lambdas;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.olah.gcloud.backup.api.DefaultApi;
import com.olah.gcloud.backup.api.model.Photo;
import com.olah.gcloud.backup.api.model.PhotoList;
import com.olah.gcloud.backup.api.model.PhotoQueryRequest;
import com.olah.gcloud.backup.server.dao.PhotoDao;
import com.olah.gcloud.backup.syncer.utils.ConfigProvider;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


import java.io.*;
import java.util.LinkedList;
import java.util.Set;

public class GetPhotoListLambda implements DefaultApi, RequestStreamHandler {


    // https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-create-api-as-simple-proxy-for-lambda.html#api-gateway-proxy-integration-lambda-function-java
    JSONParser parser = new JSONParser();

    PhotoDao photoDao = new PhotoDao(ConfigProvider.getAmazonDynamoDB(), ConfigProvider.getTableName());

    @Override
    public void addOrUpdatePhoto(Photo body) {
        throw new RuntimeException("Not supported");
    }

    @Override
    public PhotoList getPhotoByFolderAndFileName(PhotoQueryRequest input) {
        System.out.println("GetPhotoByFolderAndFileName called with " + input);
        if (input.getStatus() == null) {
            Set<Photo> photos = photoDao.getPhotosByFolder(input.getFolderPath());
            PhotoList result = getPhotoList(photos);
            return result;
        }

        Set<Photo> photos = photoDao.getPhotosByFolderAndStatus(input.getFolderPath(), Photo.StatusEnum.fromValue(input.getStatus()));
        PhotoList result = getPhotoList(photos);
        return result;
    }

    private PhotoList getPhotoList(Set<Photo> photos) {
        PhotoList result = new PhotoList();
        result.setData(new LinkedList<>());
        photos.forEach(y -> result.addDataItem(y));
        return result;
    }


    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
        LambdaLogger logger = context.getLogger();
        logger.log("Loading Java Lambda handler of ProxyWithStream");


        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        JSONObject responseJson = new JSONObject();

        try {
            JSONObject event = (JSONObject) parser.parse(reader);

            if (event.get("body") != null) {
                JSONObject body = (JSONObject) parser.parse((String) event.get("body"));

                PhotoQueryRequest photoQueryRequest = new PhotoQueryRequest();

                photoQueryRequest.setFolderPath((String) body.get("folderPath"));
                if (body.get("status") != null) {
                    photoQueryRequest.setStatus(PhotoQueryRequest.StatusEnum.fromValue((String) body.get("status")));
                }

                PhotoList photoByFolderAndFileName = getPhotoByFolderAndFileName(photoQueryRequest);
                Gson gson = new GsonBuilder().setPrettyPrinting().create();


                JSONObject headerJson = new JSONObject();
                headerJson.put("Content-Type", "application/json");

                responseJson.put("isBase64Encoded", false);
                responseJson.put("statusCode", "200");
                responseJson.put("headers", headerJson);
                responseJson.put("body", gson.toJson(photoByFolderAndFileName).replace("SYNCED","synced"));
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }


        logger.log(responseJson.toJSONString());
        OutputStreamWriter writer = new OutputStreamWriter(output, "UTF-8");
        writer.write(responseJson.toJSONString());
        writer.close();

    }

}
