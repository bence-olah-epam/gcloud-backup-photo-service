package com.olah.gcloud.backup.server.dynamodb;

import com.olah.gcloud.backup.server.dynamodb.PhotoDao;
import org.junit.Test;

public class PhotoDaoTest {

    PhotoDao photoDao = new PhotoDao("http://localhost:9000");

    @Test
    public void testtableCreation() {
        photoDao.createTable("DeviceStatus");
    }

    public void testInsertAndQuery() {

    }


}
