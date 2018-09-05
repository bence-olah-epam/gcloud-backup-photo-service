package com.olah.gcloud.backup.server.dao;

import com.olah.gcloud.backup.api.model.Photo;
import com.olah.gcloud.backup.server.utils.ConfigProvider;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import sun.security.krb5.Config;

import java.util.Set;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PhotoDaoTest {

    private PhotoDao photoDao = new PhotoDao(ConfigProvider.getAmazonDynamoDB(), ConfigProvider.getTableName());


    @Test
    public void _testtableCreation() {
        if(!ConfigProvider.getEnvironment().equals(ConfigProvider.Environment.PROD)) {
            photoDao.deleteTable();
        }
        photoDao.createTableIfNotExist();
    }

    @Test
    public void getPhoto_onePhotoStored_photoCanBeStoredAndQueried() {
        Photo photo = new Photo().fileName("sample.jpg").folderPath("folder123").status(Photo.StatusEnum.NOT_SYNCED);
        photoDao.storeOrUpdatePhoto(photo);


        Photo returnedPhoto = photoDao.getPhoto("folder123", "sample.jpg");


        Assert.assertTrue(EqualsBuilder.reflectionEquals(photo, returnedPhoto, false));
    }

    @Test
    public void getPhotosByFolderAndStatus_singlePhotoStored_singlePhotoCanBeFoundByFolderAndStatus() {
        Photo photo = new Photo().fileName("sample.jpg").folderPath("folder123").status(Photo.StatusEnum.NOT_SYNCED);
        photoDao.storeOrUpdatePhoto(photo);


        Set<Photo> result = photoDao.getPhotosByFolderAndStatus("folder123", Photo.StatusEnum.NOT_SYNCED);

        Assert.assertTrue(EqualsBuilder.reflectionEquals(photo, result.iterator().next(), false));
        Assert.assertEquals(1, result.size());
    }


    @Test
    public void getPhotosByFolderAndStatus_multiplePhotosWithDifferentState_verifyFilteringByStateWorks() {
        String folderPath = "folder_multiple_hits";
        Photo photo = new Photo().fileName("sample1.jpg").folderPath(folderPath).status(Photo.StatusEnum.NOT_SYNCED);
        photoDao.storeOrUpdatePhoto(photo);
        photo = new Photo().fileName("sample2.jpg").folderPath(folderPath).status(Photo.StatusEnum.FAILED);
        photoDao.storeOrUpdatePhoto(photo);
        photo = new Photo().fileName("sample3.jpg").folderPath(folderPath).status(Photo.StatusEnum.NOT_SYNCED);
        photoDao.storeOrUpdatePhoto(photo);

        Set<Photo> result = photoDao.getPhotosByFolderAndStatus(folderPath, Photo.StatusEnum.NOT_SYNCED);

        Assert.assertEquals(2, result.size());
    }


    @Test
    public void getPhotosByFolderAndStatus_samePhotoUpdated_latestPhotoStateReturned() {
        String folderPath = "folder_update";
        Photo photo = new Photo().fileName("sample.jpg").folderPath(folderPath).status(Photo.StatusEnum.NOT_SYNCED);
        photoDao.storeOrUpdatePhoto(photo);
        photo = new Photo().fileName("sample.jpg").folderPath(folderPath).status(Photo.StatusEnum.FAILED);
        photoDao.storeOrUpdatePhoto(photo);

        Set<Photo> result = photoDao.getPhotosByFolderAndStatus(folderPath, Photo.StatusEnum.FAILED);

        Assert.assertEquals(1, result.size());
        Assert.assertTrue(EqualsBuilder.reflectionEquals(photo, result.iterator().next(), false));
    }

    @Test
    public void getPhotosByFolderAndStatus_multipleFolderWithPhotos_onlySelectedPhotoFound() {
        String folderPath = "folder_filtering";
        Photo photo = new Photo().fileName("sample.jpg").folderPath(folderPath).status(Photo.StatusEnum.NOT_SYNCED);
        photoDao.storeOrUpdatePhoto(photo);
        photo = new Photo().fileName("sample2.jpg").folderPath(folderPath).status(Photo.StatusEnum.NOT_SYNCED);
        photoDao.storeOrUpdatePhoto(photo);
        photo = new Photo().fileName("sample.jpg").folderPath("different_folder").status(Photo.StatusEnum.NOT_SYNCED);
        photoDao.storeOrUpdatePhoto(photo);

        Set<Photo> result = photoDao.getPhotosByFolderAndStatus(folderPath, Photo.StatusEnum.NOT_SYNCED);

        Assert.assertEquals(2, result.size());
    }










    @Test
    public void getPhotosByFolder_singlePhotoStored_singlePhotoCanBeFoundByFolderAndStatus() {
        String folderPath = "folder1234";
        Photo photo = new Photo().fileName("sample.jpg").folderPath(folderPath).status(Photo.StatusEnum.NOT_SYNCED);
        photoDao.storeOrUpdatePhoto(photo);


        Set<Photo> result = photoDao.getPhotosByFolder(folderPath);

        Assert.assertTrue(EqualsBuilder.reflectionEquals(photo, result.iterator().next(), false));
        Assert.assertEquals(1, result.size());
    }


    @Test
    public void getPhotosByFolder_multiplePhotosWithDifferentState_verifyFilteringByStateWorks() {
        String folderPath = "folder_multiple_hits_2";
        Photo photo = new Photo().fileName("sample1.jpg").folderPath(folderPath).status(Photo.StatusEnum.NOT_SYNCED);
        photoDao.storeOrUpdatePhoto(photo);
        photo = new Photo().fileName("sample2.jpg").folderPath(folderPath).status(Photo.StatusEnum.FAILED);
        photoDao.storeOrUpdatePhoto(photo);
        photo = new Photo().fileName("sample3.jpg").folderPath(folderPath).status(Photo.StatusEnum.NOT_SYNCED);
        photoDao.storeOrUpdatePhoto(photo);

        Set<Photo> result = photoDao.getPhotosByFolder(folderPath);

        Assert.assertEquals(3, result.size());
    }


    @Test
    public void getPhotosByFolder_samePhotoUpdated_latestPhotoStateReturned() {
        String folderPath = "folder_update_2";
        Photo photo = new Photo().fileName("sample.jpg").folderPath(folderPath).status(Photo.StatusEnum.NOT_SYNCED);
        photoDao.storeOrUpdatePhoto(photo);
        photo = new Photo().fileName("sample.jpg").folderPath(folderPath).status(Photo.StatusEnum.FAILED);
        photoDao.storeOrUpdatePhoto(photo);

        Set<Photo> result = photoDao.getPhotosByFolder(folderPath);

        Assert.assertEquals(1, result.size());
        Assert.assertTrue(EqualsBuilder.reflectionEquals(photo, result.iterator().next(), false));
    }

    @Test
    public void getPhotosByFolder_multipleFolderWithPhotos_onlySelectedPhotoFound() {
        String folderPath = "folder_filtering_2";
        Photo photo = new Photo().fileName("sample.jpg").folderPath(folderPath).status(Photo.StatusEnum.NOT_SYNCED);
        photoDao.storeOrUpdatePhoto(photo);
        photo = new Photo().fileName("sample2.jpg").folderPath(folderPath).status(Photo.StatusEnum.NOT_SYNCED);
        photoDao.storeOrUpdatePhoto(photo);
        photo = new Photo().fileName("sample.jpg").folderPath("different_folder").status(Photo.StatusEnum.NOT_SYNCED);
        photoDao.storeOrUpdatePhoto(photo);

        Set<Photo> result = photoDao.getPhotosByFolder(folderPath);

        Assert.assertEquals(2, result.size());
    }
}