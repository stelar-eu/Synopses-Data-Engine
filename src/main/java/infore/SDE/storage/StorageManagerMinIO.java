package infore.SDE.storage;

import infore.SDE.synopses.Synopsis;
import io.minio.*;
import io.minio.errors.*;
import io.minio.messages.Item;

import java.io.*;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

public class StorageManagerMinIO {

    private static final String BUCKET_NAME = "sde-e";
    private static final String ENDPOINT = "http://192.168.1.104:10000";
    private static final String ACCESS_KEY = "4Xyy5GjeOlirSxeHwTRj";
    private static final String SECRET_KEY = "";

    private static final MinioClient minioClient = MinioClient.builder()
            .endpoint(ENDPOINT)
            .credentials(ACCESS_KEY, SECRET_KEY)
            .build();

    /**
     * This method serializes a Synopsis object irrelevant of type (AMS, LSH, CM etc)
     * and saves the serialization output to a MinIO bucket.
     *
     * @param synopsis Any synopsis object implementing the Serializable interface and
     *                 both of the writeObject() and readObject() methods.
     *
     * @param storageKeyName The name/key of the file in the MinIO bucket to store the synopsis
     *                       state under
     */
    public static void serializeSynopsisToS3(Synopsis synopsis, String storageKeyName) {
        File tempFile = new File(storageKeyName);
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(tempFile))) {
            oos.writeObject(synopsis);
            minioClient.uploadObject(
                    UploadObjectArgs.builder()
                            .bucket(BUCKET_NAME)
                            .object(storageKeyName)
                            .filename(tempFile.getAbsolutePath())
                            .build());
        } catch (IOException | MinioException | NoSuchAlgorithmException | InvalidKeyException e) {
            e.printStackTrace();
        } finally {
            tempFile.delete();
        }
    }

    /**
     * This method looks for an entry inside the MinIO bucket based on the name/key given as argument. It returns
     * a deserialized synopsis object of the specified synopsis class.
     * @param storageKeyName The key to look for inside the MinIO bucket
     * @param synopsisClass The class of the synopsis object to return
     * @return Synopsis object of specific synopsis class not generic T
     */
    public static <T extends Synopsis> T deserializeSynopsisFromS3(String storageKeyName, Class<T> synopsisClass) {
        File tempFile = new File(storageKeyName);
        try {
            minioClient.downloadObject(
                    DownloadObjectArgs.builder()
                            .bucket(BUCKET_NAME)
                            .object(storageKeyName)
                            .filename(tempFile.getAbsolutePath())
                            .build());

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
        } catch (IOException | MinioException | NoSuchAlgorithmException | InvalidKeyException e) {
            e.printStackTrace();
            return null;
        } finally {
            tempFile.delete();
        }
    }

    /**
     * Prints details about the versions of the file corresponding
     * to the given key from a versioned MinIO bucket
     * @param objectKey The key/name of the MinIO entry for which the versions are requested
     * @return List of Strings containing entries for all the version with ID, timestamp and size
     */
   public static void getObjectVersions(String objectKey) throws ServerException, InsufficientDataException, ErrorResponseException, IOException, NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
       Iterable<Result<Item>> results = minioClient.listObjects(
               ListObjectsArgs.builder()
                       .bucket(BUCKET_NAME)
                       .prefix(objectKey)
                       .includeVersions(true)
                       .build());

       for(Result<Item> r: results){
           System.out.println("Result is: "+r.get().objectName()+ " v: "+r.get().versionId()+" t: "+r.get().lastModified());
       }
   }
}
