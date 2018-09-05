package com.olah.gcloud.backup.server.dynamodb;

import com.olah.gcloud.backup.api.model.Photo;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class PhotoDaoTest {

    PhotoDao photoDao = new PhotoDao("http://localhost:9000");

    @Test
    public void testtableCreation() {
        photoDao.deleteTable();
        photoDao.createTableIfNotExist();
    }


    @Test
    public void testInsertAndQuery() {
        Photo photo = new Photo().fileName("sample.jpg").folderPath("folder123").status(Photo.StatusEnum.NOT_SYNCED);
        photoDao.storeOrUpdatePhoto(photo);


        Photo returnedPhoto = photoDao.getPhoto("folder123", "sample.jpg");

        System.out.println("Photo: " + photo);
        System.out.println("Returned photo: " + returnedPhoto);


        Assert.assertTrue(EqualsBuilder.reflectionEquals(photo, returnedPhoto, false));
    }


    @Test
    public void testSearchingByState() {
        Photo photo = new Photo().fileName("sample.jpg").folderPath("folder123").status(Photo.StatusEnum.NOT_SYNCED);
        photoDao.storeOrUpdatePhoto(photo);


        Set<Photo> result = photoDao.getPhotosByFolderAndStatus("folder123", Photo.StatusEnum.NOT_SYNCED);


        Assert.assertTrue(EqualsBuilder.reflectionEquals(photo, result.iterator().next(), false));
    }


}
