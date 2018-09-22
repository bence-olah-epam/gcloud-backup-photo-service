package com.olah.gcloud.backup.server.lambdas;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.olah.gcloud.backup.api.DefaultApi;
import com.olah.gcloud.backup.api.model.Photo;
import com.olah.gcloud.backup.api.model.PhotoList;
import com.olah.gcloud.backup.api.model.PhotoQueryRequest;
import com.olah.gcloud.backup.server.dao.PhotoDao;
import com.olah.gcloud.backup.syncer.utils.ConfigProvider;

public class AddOrUpdatePhotoLambda implements DefaultApi, RequestHandler<Photo, Void> {

    PhotoDao photoDao = new PhotoDao(ConfigProvider.getAmazonDynamoDB(), ConfigProvider.getTableName());

    @Override
    public void addOrUpdatePhoto(Photo body) {
        photoDao.storeOrUpdatePhoto(body);
    }

    @Override
    public PhotoList getPhotoByFolderAndFileName(PhotoQueryRequest status) {
        throw new RuntimeException();
    }

    @Override
    public Void handleRequest(Photo input, Context context) {
        addOrUpdatePhoto(input);
        return null;
    }
}
