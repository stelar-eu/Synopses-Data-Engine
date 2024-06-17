package infore.SDE.testing;

import infore.SDE.storage.StorageManager;
import infore.SDE.storage.StorageManagerMinIO;
import infore.SDE.synopses.AMSsynopsis;
import io.minio.errors.*;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.*;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;


public class SerializationTestMinIO {

    public static void main(String[] args) {
        String[] parameters = { "1", "2", "1000", "10", "10" };
        AMSsynopsis wrapper = new AMSsynopsis(1101, parameters);

        //String uniqueFileName = UUID.randomUUID().toString() + ".ser";

        String uniqueFileName = "e0220a70-0ac1-4bc8-91d3-fc065da6ac9f.ser";

        // Serialize and upload to MinIO S3
        StorageManagerMinIO.serializeSynopsisToS3(wrapper, uniqueFileName);

        // Download from MinIO S3 and deserialize
        AMSsynopsis deserializedWrapper = StorageManagerMinIO.deserializeSynopsisFromS3(uniqueFileName, AMSsynopsis.class);
        if (deserializedWrapper != null) {
            System.out.println(deserializedWrapper);
        }

        try {
            StorageManagerMinIO.getObjectVersions(uniqueFileName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
