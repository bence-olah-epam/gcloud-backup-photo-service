package com.olah.gcloud.backup.server.lambdas;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.olah.gcloud.backup.api.DefaultApi;
import com.olah.gcloud.backup.api.model.Photo;
import com.olah.gcloud.backup.api.model.PhotoList;
import com.olah.gcloud.backup.api.model.PhotoQueryRequest;
import com.olah.gcloud.backup.server.dao.PhotoDao;
import com.olah.gcloud.backup.syncer.utils.ConfigProvider;


import java.util.Set;

public class GetPhotoListLambda implements DefaultApi, RequestHandler<PhotoQueryRequest, PhotoList> {


    // https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-create-api-as-simple-proxy-for-lambda.html#api-gateway-proxy-integration-lambda-function-java

    PhotoDao photoDao = new PhotoDao(ConfigProvider.getAmazonDynamoDB(), ConfigProvider.getTableName());

    @Override
    public void addOrUpdatePhoto(Photo body) {
        photoDao.storeOrUpdatePhoto(body);
    }

    @Override
    public PhotoList getPhotoByFolderAndFileName(PhotoQueryRequest input) {
        System.out.println("GetPhotoByFolderAndFileName called with " + input);
        if(input.getStatus() == null) {
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
        photos.forEach(y -> result.addDataItem(y));
        return result;
    }


    @Override
    public PhotoList handleRequest(PhotoQueryRequest input, Context context) {
       return getPhotoByFolderAndFileName(input);
    }

}
