package com.olah.gcloud.backup.server.lambdas;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
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

public class AddOrUpdatePhotoLambda implements DefaultApi, RequestStreamHandler {
    JSONParser parser = new JSONParser();

    PhotoDao photoDao = new PhotoDao(ConfigProvider.getAmazonDynamoDB(), ConfigProvider.getTableName());

    @Override
    public void addOrUpdatePhoto(Photo body) {
        photoDao.storeOrUpdatePhoto(body);
    }

    @Override
    public PhotoList getPhotoByFolderAndFileName(PhotoQueryRequest status) {
        throw new RuntimeException("Not supported");

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

                Photo photo = new Photo();

                photo.setFolderPath((String) body.get("folderPath"));
                photo.setStatus(Photo.StatusEnum.valueOf((String) body.get("status")));
                photo.setFileName((String) body.get("fileName"));

                addOrUpdatePhoto(photo);


                JSONObject responseBody = new JSONObject();
                responseBody.put("input", event.toJSONString());

                JSONObject headerJson = new JSONObject();

                responseJson.put("isBase64Encoded", false);
                responseJson.put("statusCode", "200");
                responseJson.put("headers", headerJson);
                responseJson.put("body", responseBody.toString());
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
