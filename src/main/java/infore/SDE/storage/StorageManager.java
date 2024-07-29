package infore.SDE.storage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.synopses.AMSsynopsis;
import infore.SDE.synopses.Synopsis;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.ResponseInputStream;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;


public class StorageManager {

    private static final String BUCKET_NAME = "sde-e";
    private static final Region REGION = Region.EU_NORTH_1;
    private static final String AWS_ACCESS_KEY_ID = "AKIAS2XH2Y6RWKPC3QHH";
    private static final String AWS_SECRET_ACCESS_KEY = "";

    // Parse the input JSON string into a JsonNode
    private static final S3Client s3 =  S3Client.builder()
            .region(REGION)
            .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)))
            .build();

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Abstracts the whole process of snapshotting a synopsis and generating a new version of
     * it inside the storage bucket in S3. It handles the generation of metadata, appropriate
     * version handling and storage.
     * @param synopsis The synopsis to be snapshot
     * @return boolean True if process was successful, else false
     */
    public static boolean snapshotSynopsis(Synopsis synopsis, String datasetKey) {

        // Assert that synopsis object given is not null, based.
        if(synopsis==null){
            return false;
        }

        try {
            // Generate the key prefix for all synopsis related objects.
            String synopsisStorageKey = "syn_"+ synopsis.getSynopsisID()+"_"+datasetKey+"_";

            // Structure OR update the metadata of the synopsis or newly added version of it and get
            // the version number that should be used, calculated by the metadata handling function.
            int newVersionNumber = buildOrUpdateSynopsisMetadata(synopsis, datasetKey);
            synopsisStorageKey += "v"+newVersionNumber;

            // Serialize the synopsis object into the S3 bucket with .ser suffix as key
            serializeSynopsisToS3(synopsis, synopsisStorageKey+".ser");

            // Snapshot the state of the synopsis in JSON format into the S3 bucket with .json suffix as key
            storeSnapshotOfFormatInS3(synopsis, synopsisStorageKey+".json");

        }catch(Exception e){
            e.printStackTrace();
            return false;
        }

        // The process of version snapshot has been completed at this point, so True should be returned
        // to mark the completion for the caller
        return true;
    }

    /**
     * This method serializes a Synopsis object irrelevant of type (AMS, LSH, CM etc.)
     * and saves the serialization output to an S3 bucket.
     *
     * @param synopsis Any synopsis object implementing the Serializable interface and
     *                 both of the writeObject() and readObject() methods.
     *
     * @param storageKeyName The name/key of the file in the S3 bucket to store the synopsis
     *                       state under
     */
    private static void serializeSynopsisToS3(Synopsis synopsis, String storageKeyName){
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
     * This method looks for an entry inside the S3 bucket based on the name/key given as argument. It returns
     * a deserialized synopsis object of the specified synopsis class.
     * @param storageKeyName The key to look for inside the S3 bucket
     * @param synopsisClass The class of the synopsis object to return
     * @return Synopsis object of specific synopsis class not generic T
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


    /**
     * This method stores the state of a currently maintained Synopsis from Flink
     * (including all member variables state etc.) in JSON format (for example)
     * into S3.
     *
     * @param synopsis Any synopsis object implementing the toJson() method with
     *                 custom logic per synopsis case
     *
     * @param storageKeyName The name/key of the file in the S3 bucket to store the synopsis
     *                       state under using appropriate suffix.
     *
     * //TODO Under developement, this instance of the method only work for AMS Sketches
     */
    public static void storeSnapshotOfFormatInS3(Synopsis synopsis, String storageKeyName){
        File tempFile = new File(storageKeyName);
        AMSsynopsis amsSynopsis = (AMSsynopsis) synopsis;
        try {
            String json = amsSynopsis.toJson();

            // Write JSON to a temporary file
            Files.write(Paths.get(tempFile.getAbsolutePath()), json.getBytes());

            // Upload the file to S3
            s3.putObject(PutObjectRequest.builder().bucket(BUCKET_NAME).key(storageKeyName).build(),
                    RequestBody.fromFile(tempFile));

            try {
                // Delete the temporary file
                Files.delete(tempFile.toPath());
            }catch(Exception e){}

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * Abstracts the whole process of loading a snapshot of a synopsis in the form
     * of Java object from an S3 bucket, in which it was previously saved. The use
     * of the object returned (if so), is subject to the caller of the method.
     *
     * @param datasetKey The dataset key defined by the caller of the method
     * @param uid The unique id of the synopsis to be loaded
     * @param synopsisClass The class of the synopsis object to return
     * @param versionNumber The snapshot version requested
     * @return Synopsis object of specific synopsis class not generic T in version 'versionNumber' or null if not found
     */
    public static <T extends Synopsis> T loadSynopsisSnapshot(String datasetKey, int uid, Class<T> synopsisClass, int versionNumber) {
        String snapshotKey = "syn_"+uid+"_"+datasetKey+"_v"+versionNumber+".ser";
        try{
            return deserializeSynopsisFromS3(snapshotKey, synopsisClass);
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Abstracts the whole process of loading the latest snapshot of a synopsis in the form
     * of Java object from an S3 bucket, in which it was previously saved. The use
     * of the object returned (if so), is subject to the caller of the method.
     *
     * @param uid The unique identifier of the synopsis to load the latest snapshot of
     * @param datasetKey The request dataset parameter
     * @param synopsisClass The class of the synopsis object to be loaded
     * @return Synopsis object of specific synopsis class not generic T or null in case of error
     */
    public static <T extends Synopsis> T loadSynopsisLatestSnapshot(String datasetKey, int uid, Class<T> synopsisClass) {
        try {
            String snapshotKey = getSynopsisVersions(uid, datasetKey).get(0);
            return deserializeSynopsisFromS3(snapshotKey, synopsisClass);
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Abstracts the whole process of loading the latest state of a synopsis in the form
     * of Java object from an S3 bucket, in which it was previously saved. The use
     * of the object returned (if so), is subject to the caller of the method.
     *
     * @param s The synopsis to load the latest state snapshot for, must be already snapshot once in the past
     * @param datasetKey The request dataset parameter
     * @return JSON in string format
     */
    public static String loadSynopsisLatestState(Synopsis s, String datasetKey) {
        try {
            int latestVersion = getSynopsisLatestVersionNumber(s.getSynopsisID(), datasetKey);
            String stateKey = "syn_"+s.getSynopsisID()+"_"+datasetKey+"_v"+latestVersion+".json";
            return getObjectFromS3(stateKey);
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }


    /**
     * Method to put (or overwrite) object with key=storageKeyName using content for
     * its value, in an S3 bucket.
     * @param storageKeyName The key of the object
     * @param content The value is String format
     */
    private static void putObjectToS3(String storageKeyName, String content) {

        try {
            // Upload the file to S3
            s3.putObject(PutObjectRequest.builder().bucket(BUCKET_NAME).key(storageKeyName).build(),
                    RequestBody.fromBytes(content.getBytes()));
        } catch (S3Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Method to get the content of a specific object using its key from an
     * S3 bucket.
     * @param storageKeyName The key of the object
     * @return The content of the object in String format
     */
    private static String getObjectFromS3(String storageKeyName) {
        StringBuilder content = new StringBuilder();

        try {
            // Check if the object exists
            HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(storageKeyName)
                    .build();

            HeadObjectResponse headObjectResponse = s3.headObject(headObjectRequest);

            // If the object exists, proceed to get the object
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(storageKeyName)
                    .build();

            ResponseInputStream<?> response = s3.getObject(getObjectRequest);

            BufferedReader reader = new BufferedReader(new InputStreamReader(response, StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }

        } catch (S3Exception | IOException e) {}

        return content.toString();
    }


    /**
     * Returns the entire JSON metadata structure for a given synopsis
     * which has already been snapshot at least once in the past.
     * @param uid The synopsis unique identifier
     * @param datasetKey The request dataset key parameter
     * @return The JSON metadata in form of String.
     */
    public static String getSynopsisMetadata(int uid, String datasetKey){
        String synMetadataKey = "syn_"+uid+"_"+datasetKey+".METADATA.json";
        String metadata = getObjectFromS3(synMetadataKey);
        return metadata;
    }

    /**
     * Returns the number of the latest version of a given synopsis object previously
     * snapshot into the S3 bucket
     * @param uid The synopsis unique identifier
     * @param datasetKey The request dataset key parameter
     * @return Number of the latest version
     */
    public static int getSynopsisLatestVersionNumber(int uid, String datasetKey) {
        String metadata = getSynopsisMetadata(uid, datasetKey);
        if(!metadata.equals(null) && !metadata.isEmpty()){
            JsonNode rootNode = null;
            try {
                rootNode = objectMapper.readTree(metadata);
                // Get the latest version number
                JsonNode latestVersionNode = rootNode.path("current_version");
                // Check if the latest version exists
                if (latestVersionNode.isMissingNode()) {
                    return -1;
                }
                // Return the latest version number as an integer
                return latestVersionNode.asInt();
            } catch (IOException e) {
                return -1;
            }
        }
        return -1;
    }

    /**
     * Returns the keys corresponding to the version files of the synopsis s
     * (.ser version files) from an S3 bucket. The file keys are returned as
     * a list of Strings in descending order (from latest to oldest).
     * @param uid The synopsis unique identifier
     * @param datasetKey The request dataset key parameter
     * @return List of Strings containing all the versions of the synopsis
     * @throws IOException
     */
    public static List<String> getSynopsisVersions(int uid, String datasetKey) throws IOException {
        String jsonString = getSynopsisMetadata(uid, datasetKey);
        // Parse the input JSON string into a JsonNode
        ObjectNode rootNode = (ObjectNode) objectMapper.readTree(jsonString);
        ObjectNode versionsNode = (ObjectNode) rootNode.path("versions");
        // Collect file names
        List<String> fileNames = new ArrayList<>();
        Iterator<String> fieldNames = versionsNode.fieldNames();
        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();
            JsonNode versionNode = versionsNode.get(fieldName);
            String fileName = versionNode.get("file_name").asText();
            fileNames.add(fileName);
        }

        // Sort file names in descending order
        Collections.sort(fileNames, Collections.reverseOrder());
        return fileNames;
    }

    /**
     * Builds or updates the JSON metadata file for a synopsis including info
     * about the versions of the synopsis, timestamps, newest version identifier etc.
     * The resulting file is put into the S3 bucket with suffix: .METADATA. with
     * its contents built in JSON format. You may reference to the documentation for the
     * specific structure of this metadata file.
     * @param s The synopsis object to build metadata file for
     * @param datasetKey The request dataset key parameter
     * @return The assigned number of the newly added version to be added to the bucket by
     *         snapshotSynopsis()
     */
    public static int buildOrUpdateSynopsisMetadata(Synopsis s, String datasetKey) throws IOException {
        String synMetadataKey = "syn_"+s.getSynopsisID()+"_"+datasetKey+".METADATA.json";
        String synopsisStorageKeyPrefix = "syn_"+ s.getSynopsisID()+"_"+datasetKey+"_";

        // Try to get the metadata
        String metadata = getSynopsisMetadata(s.getSynopsisID(), datasetKey);

        // If synopsis is totally new and has not metadata
        if(metadata.equals(null) || metadata.isEmpty()){
        // Metadata for this specific synopsis have not been created until now, they
        // should be constructed now

            // Create an ObjectMapper instance
            ObjectMapper mapper = new ObjectMapper();

            // Create the root node
            ObjectNode rootNode = mapper.createObjectNode();
            rootNode.put("file_prefix", synopsisStorageKeyPrefix);
            rootNode.put("current_version", "0");
            rootNode.put("type", s.getClass().getSimpleName());

            // Create the versioned_states node
            ObjectNode versionedStatesNode = mapper.createObjectNode();
            ObjectNode versionedState0 = mapper.createObjectNode();
            versionedState0.put("file_name", synopsisStorageKeyPrefix+"v0.json");
            versionedState0.put("snapshot_at", Instant.now().toString());
            versionedStatesNode.set("0", versionedState0);
            rootNode.set("versioned_states", versionedStatesNode);

            // Create the versions node
            ObjectNode versionsNode = mapper.createObjectNode();
            ObjectNode version0 = mapper.createObjectNode();
            version0.put("file_name", synopsisStorageKeyPrefix+"v0.ser");
            version0.put("snapshot_at", Instant.now().toString());
            versionsNode.set("0", version0);
            rootNode.set("versions", versionsNode);

            putObjectToS3(synMetadataKey, mapper.writeValueAsString(rootNode));

            //This is the first version of the synopsis, so we return 0
            return 0;
        }else{

        // Synopsis metadata have been priorly generated, so they just need to be updated
        // by registering the newly created snapshot with the respective version.

            ObjectMapper mapper = new ObjectMapper();

            // Step 1: Keep the JSON metadata file from the bucket
            // Parse the existing JSON string
            JsonNode rootNode = mapper.readTree(metadata);

            // Step 2a: Get the current version of the synopsis and update it with the new one
            String newVersion = Integer.toString(getSynopsisLatestVersionNumber(s.getSynopsisID(),datasetKey) + 1);

            // Step 2b: Generate the object/files that will also be used by the snapshotSynopsis()
            //          to maintain consistency along the metadata and the actual storage

            // Using synopsisStorageKeyPrefix already defined in this scope

            // Step 3: Add entries into the "versions" and "versioned_states" objects of the JSON
            // for the newly added version

            // Add a new versioned_state
            ObjectNode versionedStatesNode = (ObjectNode) rootNode.get("versioned_states");
            ObjectNode newState = mapper.createObjectNode();
            newState.put("file_name", synopsisStorageKeyPrefix+"v"+newVersion+".json");
            newState.put("snapshot_at", Instant.now().toString());
            versionedStatesNode.set(newVersion, newState);

            // Add a new version
            ObjectNode versionsNode = (ObjectNode) rootNode.get("versions");
            ObjectNode newVersionNode = mapper.createObjectNode();
            newVersionNode.put("file_name", synopsisStorageKeyPrefix+"v"+newVersion+".ser");
            newVersionNode.put("snapshot_at", Instant.now().toString());
            versionsNode.set(newVersion, newVersionNode);

            // Step 4: Update the reference to the current version
            ((ObjectNode) rootNode).put("current_version", newVersion);

            putObjectToS3(synMetadataKey, mapper.writeValueAsString(rootNode));

            return Integer.valueOf(newVersion);
        }
    }
}
