package infore.SDE.storage;

import infore.SDE.synopses.AMSsynopsis;
import infore.SDE.synopses.Synopsis;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.*;

public class StorageManager {

    private static final String BUCKET_NAME = "sde-e";
    private static final Region REGION = Region.EU_NORTH_1;
    private static final String AWS_ACCESS_KEY_ID = "AKIAS2XH2Y6RRAT6LFZF";
    private static final String AWS_SECRET_ACCESS_KEY = "";


    private static final S3Client s3 =  S3Client.builder()
            .region(REGION)
            .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)))
            .build();

    /**
     * This method serializes a Synopsis object irrelevant of type (AMS, LSH, CM etc)
     * and saves the serialization output to an S3 bucket.
     *
     * @param synopsis Any synopsis object implementing the Serializable interface and
     *                 both of the writeObject() and readObject() methods.
     *
     * @param storageKeyName The name/key of the file in the S3 bucket to store the synopsis
     *                       state under
     */
    public static void serializeSynopsisToS3(Synopsis synopsis, String storageKeyName){
        File tempFile = new File(storageKeyName);
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(tempFile))) {
            oos.writeObject(synopsis);
            s3.putObject(PutObjectRequest.builder().bucket(BUCKET_NAME).key(storageKeyName).build(),
                    RequestBody.fromFile(tempFile));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            tempFile.delete();
        }
    }


    /**
     * This looks for version
     * @param storageKeyName
     * @param synopsisClass
     * @return
     * @param <T>
     */
    public static <T extends Synopsis> T deserializeSynopsisFromS3(String storageKeyName, Class<T> synopsisClass) {
        File tempFile = new File(storageKeyName);
        try {
            s3.getObject(GetObjectRequest.builder().bucket(BUCKET_NAME).key(storageKeyName).build(),
                    ResponseTransformer.toFile(tempFile.toPath()));

            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(tempFile))) {
                Object obj = ois.readObject();
                if (synopsisClass.isInstance(obj)) {
                    return synopsisClass.cast(obj);
                } else {
                    throw new ClassNotFoundException("Deserialized object is not of type " + synopsisClass.getName());
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
                return null;
            }
        } finally {
            tempFile.delete();
        }
    }
}
