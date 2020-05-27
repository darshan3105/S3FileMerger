package com.github.darshan3105.helpers;

import static com.github.darshan3105.constants.CommonConstants.MAXIMUM_PART_SIZE;
import static com.github.darshan3105.constants.CommonConstants.NEW_LINE;
import static com.github.darshan3105.utils.S3FileMergingUtils.generateFileMergingUploadResponse;
import static com.github.darshan3105.utils.S3FileMergingUtils.generateMergedFileS3Key;
import static com.github.darshan3105.utils.S3FileMergingUtils.generateMultiPartUploadRequest;
import static com.github.darshan3105.utils.S3FileMergingUtils.generateMultiPartUploadResponse;
import static com.github.darshan3105.utils.S3FileMergingUtils.mergeS3Files;
import static com.github.darshan3105.utils.S3Utils.chunkBySize;
import static com.github.darshan3105.utils.S3Utils.getFileFormat;
import static com.github.darshan3105.utils.S3Utils.shouldUseMultiPartUpload;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.io.IOUtils;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.github.darshan3105.exceptions.FileMergingException;
import com.github.darshan3105.models.FileMergingUploadRequest;
import com.github.darshan3105.models.FileMergingUploadResponse;
import com.github.darshan3105.models.MultiPartUploadRequest;
import com.github.darshan3105.models.MultiPartUploadResponse;
import com.github.darshan3105.models.S3KeyInfo;
import com.github.darshan3105.models.S3Parts;
import com.github.darshan3105.utils.S3FileMergingUtils;

import lombok.NonNull;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class FileMergingUploadHelper {

    private static final int FIRST_PART = 1;
    private static final boolean REMOVE_HEADER = true;
    private static final boolean DO_NOT_REMOVE_HEADER = false;
    private static final int MAXIMUM_THREAD_COUNT = 30;

    private final S3Helper s3Helper;

    @Inject
    public FileMergingUploadHelper(@NonNull final S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    public FileMergingUploadResponse upload(
        @NonNull final FileMergingUploadRequest fileMergingUploadRequest) {
        if (shouldUseMultiPartUpload(fileMergingUploadRequest.getS3KeysInfo())) {
            return handleMultiPartUpload(fileMergingUploadRequest);
        } else {
            return handleNormalUpload(fileMergingUploadRequest);
        }
    }

    private FileMergingUploadResponse handleNormalUpload(
        final FileMergingUploadRequest fileMergingUploadRequest) {
        final String mergedFileS3Key =
            generateMergedFileS3Key(fileMergingUploadRequest.getMergedFileS3KeyPrefix(),
                getFileFormat(fileMergingUploadRequest.getS3KeysInfo().get(0).getKeyName()));
        List<String> s3Keys = fileMergingUploadRequest.getS3KeysInfo().stream()
            .map(S3KeyInfo::getKeyName).collect(Collectors.toList());
        List<List<String>> s3ObjectsContent = getS3ObjectsContent(
            fileMergingUploadRequest.getSourceBucketName(), s3Keys);
        String mergedObject = getMergedObject(fileMergingUploadRequest.isShouldManageHeaders(),
            s3ObjectsContent, FIRST_PART);
        s3Helper.uploadObject(fileMergingUploadRequest.getDestinationBucketName(), mergedFileS3Key,
            mergedObject);
        return generateFileMergingUploadResponse(
            fileMergingUploadRequest.getDestinationBucketName(), mergedFileS3Key);
    }

    private FileMergingUploadResponse handleMultiPartUpload(
        final FileMergingUploadRequest fileMergingUploadRequest) {
        final String sourceBucketName = fileMergingUploadRequest.getSourceBucketName();
        final String destinationBucketName = fileMergingUploadRequest.getDestinationBucketName();
        final String mergedFileS3Key =
            generateMergedFileS3Key(fileMergingUploadRequest.getMergedFileS3KeyPrefix(),
                getFileFormat(fileMergingUploadRequest.getS3KeysInfo().get(0).getKeyName()));
        S3Parts s3Parts = chunkBySize(fileMergingUploadRequest.getS3KeysInfo());
        String uploadId = s3Helper.startMultiPartUpload(destinationBucketName, mergedFileS3Key);
        try {
            MultiPartUploadResponse largeS3KeysMultiPartResponse =
                handleMultiPartUploadForKeysAboveMaximumPartSize(
                    generateMultiPartUploadRequest(sourceBucketName, destinationBucketName,
                        uploadId, s3Parts, 1,
                        fileMergingUploadRequest.isShouldManageHeaders()), mergedFileS3Key);
            MultiPartUploadResponse smallS3KeysMultiPartResponse =
                handleMultiPartUploadForKeysBelowMaximumPartSize(
                    generateMultiPartUploadRequest(sourceBucketName, destinationBucketName,
                        uploadId, s3Parts, largeS3KeysMultiPartResponse.getNextPartNumber(),
                        fileMergingUploadRequest.isShouldManageHeaders()), mergedFileS3Key);
            List<PartETag> partETags = getPartETags(smallS3KeysMultiPartResponse.getPartETags(),
                largeS3KeysMultiPartResponse.getPartETags());
            s3Helper.completeMultiPartUpload(destinationBucketName, mergedFileS3Key, uploadId,
                partETags);
            return generateFileMergingUploadResponse(destinationBucketName, mergedFileS3Key);
        } catch (Exception e) {
            String errorMessage = String.format(
                "Aborting the multipart upload for request %s", fileMergingUploadRequest);
            log.error(errorMessage, e);
            s3Helper.abortMultiPartUpload(destinationBucketName, mergedFileS3Key, uploadId);
            throw new FileMergingException(errorMessage, e);
        }
    }

    private List<PartETag> getPartETags(final List<PartETag> smallUploadPartETags,
        final List<PartETag> largeUploadPartETags) {
        List<PartETag> partETags = new ArrayList<>();
        if (!smallUploadPartETags.isEmpty()) {
            partETags.addAll(smallUploadPartETags);
        }
        if (!largeUploadPartETags.isEmpty()) {
            partETags.addAll(largeUploadPartETags);
        }
        return partETags;
    }

    private MultiPartUploadResponse handleMultiPartUploadForKeysAboveMaximumPartSize(
        final MultiPartUploadRequest multiPartUploadRequest, final String mergerFileS3Key) {
        final String sourceBucketName = multiPartUploadRequest.getSourceBucketName();
        final String destinationBucketName = multiPartUploadRequest.getDestinationBucketName();
        List<PartETag> partETags = new ArrayList<>();
        List<String> s3Keys =
            multiPartUploadRequest.getS3Parts().getKeysAboveMaximumPartSize().stream()
                .map(S3KeyInfo::getKeyName).collect(Collectors.toList());
        final String uploadId = multiPartUploadRequest.getUploadId();
        int partNumber = multiPartUploadRequest.getStartPartNumber();
        for (final String s3Key : s3Keys) {
            S3Object s3Object = s3Helper.getS3Object(sourceBucketName, s3Key);
            Scanner sc = new Scanner(s3Object.getObjectContent(), StandardCharsets.UTF_8.name())
                .useDelimiter(NEW_LINE);
            long currentSize = 0L;
            List<String> currentPart = new ArrayList<>();
            while (sc.hasNext()) {
                String currentLine = sc.next();
                currentSize += currentLine.getBytes(Charset.defaultCharset()).length;
                if (currentSize < MAXIMUM_PART_SIZE) {
                    currentPart.add(currentLine);
                } else {
                    List<List<String>> content = Arrays.asList(currentPart);
                    String mergedContent = getMergedObject(
                        multiPartUploadRequest.isShouldManageHeaders(), content, partNumber);
                    partETags.add(s3Helper.uploadPart(destinationBucketName, mergerFileS3Key,
                        uploadId, partNumber, mergedContent));
                    currentSize = currentLine.getBytes(Charset.defaultCharset()).length;
                    currentPart = new ArrayList<String>(){{
                        add(currentLine);
                    }};
                    partNumber++;
                }
            }
            if(!currentPart.isEmpty()){
                List<List<String>> content = Arrays.asList(currentPart);
                String mergedContent = getMergedObject(
                    multiPartUploadRequest.isShouldManageHeaders(), content, partNumber);
                partETags.add(s3Helper.uploadPart(destinationBucketName, mergerFileS3Key,
                    uploadId, partNumber, mergedContent));
                partNumber++;
            }
        }
        return generateMultiPartUploadResponse(partNumber, partETags);
    }

    private MultiPartUploadResponse handleMultiPartUploadForKeysBelowMaximumPartSize(
        final MultiPartUploadRequest multiPartUploadRequest, final String mergedFileS3Key) {
        List<PartETag> partETags = new ArrayList<>();
        List<List<S3KeyInfo>> chunks = multiPartUploadRequest.getS3Parts()
            .getKeysBelowMaximumPartSize();
        final String uploadId = multiPartUploadRequest.getUploadId();
        int partNumber = multiPartUploadRequest.getStartPartNumber();
        for (List<S3KeyInfo> chunk : chunks) {
            List<String> s3Keys = chunk.stream().map(S3KeyInfo::getKeyName)
                .collect(Collectors.toList());
            List<List<String>> s3ObjectsContent = getS3ObjectsContent(
                multiPartUploadRequest.getSourceBucketName(), s3Keys);
            String mergedObject = getMergedObject(multiPartUploadRequest.isShouldManageHeaders(),
                s3ObjectsContent, partNumber);
            PartETag partETag = s3Helper
                .uploadPart(multiPartUploadRequest.getDestinationBucketName(),
                    mergedFileS3Key, uploadId, partNumber, mergedObject);
            partETags.add(partETag);
            partNumber++;
        }
        return generateMultiPartUploadResponse(partNumber, partETags);
    }

    private List<List<String>> getS3ObjectsContent(final String bucketName,
        final List<String> s3Keys) {
        try {
            ForkJoinPool threadPool = new ForkJoinPool(MAXIMUM_THREAD_COUNT);
            return threadPool
                .submit(() -> s3Keys.stream().parallel().map(s3Key -> getObjectContent(bucketName,
                    s3Key)).collect(Collectors.toList())).get();
        } catch (InterruptedException | ExecutionException e) {
            String errorMessage = "Failed to get S3 objects";
            log.error(errorMessage, e);
            throw new FileMergingException(errorMessage, e);
        }
    }

    private List<String> getObjectContent(final String bucketName, final String s3Key)
        throws FileMergingException {
        try (S3Object s3Object = s3Helper.getS3Object(bucketName, s3Key)) {
            return getContent(s3Object.getObjectContent());
        } catch (AmazonServiceException | IOException e) {
            String errorMessage = String.format(
                "Failed to get the object with s3Key %s from bucket %s", s3Key, bucketName);
            log.error(errorMessage, e);
            throw new FileMergingException(errorMessage, e);
        }
    }

    private List<String> getContent(final S3ObjectInputStream objectContent) {
        BufferedReader reader = IOUtils.toBufferedReader(
            new InputStreamReader(objectContent, Charset.defaultCharset()));
        return reader.lines().collect(Collectors.toList());
    }

    private String getMergedObject(final boolean shouldManageHeaders,
        final List<List<String>> s3ObjectsContent, final int partNumber) {
        if (shouldManageHeaders) {
            StringBuilder mergedContent = new StringBuilder();
            if (partNumber == FIRST_PART) {
                mergedContent.append(S3FileMergingUtils.getFileHeader(s3ObjectsContent.get((0))));
            }
            mergedContent.append(mergeS3Files(s3ObjectsContent, REMOVE_HEADER));
            return mergedContent.toString();
        } else {
            return mergeS3Files(s3ObjectsContent, DO_NOT_REMOVE_HEADER);
        }
    }
}
