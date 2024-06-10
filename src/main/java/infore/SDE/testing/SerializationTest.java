package infore.SDE.testing;

import infore.SDE.synopses.AMSsynopsis;

import java.io.*;
import java.util.UUID;

import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;


public class SerializationTest {

    private static final String BUCKET_NAME = "sde-e";
    private static final Region REGION = Region.EU_NORTH_1;
    private static final String AWS_ACCESS_KEY_ID = "AKIAS2XH2Y6RRAT6LFZF";
    private static final String AWS_SECRET_ACCESS_KEY = "";

    private static final S3Client s3 = S3Client.builder()
            .region(REGION)
            .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)))
            .build();

    public static void main(String[] args) {
        String[] parameters = { "1", "2", "1000", "10", "10" };
        AMSsynopsis wrapper = new AMSsynopsis(1101, parameters);

        String uniqueFileName = UUID.randomUUID().toString() + ".ser";

        // Serialize and upload to S3
        serializeAndUploadToS3(wrapper, uniqueFileName);

        // Download from S3 and deserialize
        AMSsynopsis deserializedWrapper = downloadAndDeserializeFromS3(uniqueFileName);
        if (deserializedWrapper != null) {
            System.out.println(deserializedWrapper);
        }
    }

    private static void serializeAndUploadToS3(AMSsynopsis wrapper, String fileName) {
        File tempFile = new File(fileName);
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(tempFile))) {
            oos.writeObject(wrapper);
            s3.putObject(PutObjectRequest.builder().bucket(BUCKET_NAME).key(fileName).build(),
                    RequestBody.fromFile(tempFile));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            tempFile.delete();
        }
    }

    private static AMSsynopsis downloadAndDeserializeFromS3(String fileName) {
        File tempFile = new File(fileName);
        try {
            s3.getObject(GetObjectRequest.builder().bucket(BUCKET_NAME).key(fileName).build(),
                    ResponseTransformer.toFile(tempFile.toPath()));

            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(tempFile))) {
                return (AMSsynopsis) ois.readObject();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
                return null;
            }
        } finally {
            tempFile.delete();
        }
    }
}
