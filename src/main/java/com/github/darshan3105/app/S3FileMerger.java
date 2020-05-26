package com.github.darshan3105.app;

import static com.github.darshan3105.utils.S3FileMergingUtils.getS3FileMergingResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.github.darshan3105.exceptions.FileMergingException;
import com.github.darshan3105.helpers.FileMergingUploadHelper;
import com.github.darshan3105.models.FileMergingUploadRequest;
import com.github.darshan3105.models.FileMergingUploadResponse;
import com.github.darshan3105.models.S3FileMergingRequest;
import com.github.darshan3105.models.S3FileMergingResponse;
import com.github.darshan3105.models.S3KeyInfo;

import lombok.NonNull;
import lombok.extern.log4j.Log4j2;

@Log4j2
class S3FileMerger {

    private static final int MAXIMUM_THREAD_COUNT = 30;

    private final AmazonS3 amazonS3;
    private final FileMergingUploadHelper fileMergingUploadHelper;

    /**
     * @param amazonS3 of type {@link AmazonS3}
     * @param fileMergingUploadHelper of type {@link FileMergingUploadHelper}
     */
    public S3FileMerger(@NonNull final AmazonS3 amazonS3,
        @NonNull final FileMergingUploadHelper fileMergingUploadHelper) {
        this.amazonS3 = amazonS3;
        this.fileMergingUploadHelper = fileMergingUploadHelper;
    }

    /**
     * @param s3FileMergingRequest of type {@link S3FileMergingRequest}
     * @return an instance of {@link S3FileMergingResponse}
     */
    public S3FileMergingResponse handleRequest(
        @NonNull final S3FileMergingRequest s3FileMergingRequest) {
        List<S3KeyInfo> s3KeysInfo = getS3KeysInfo(s3FileMergingRequest);
        FileMergingUploadResponse fileMergingUploadResponse = fileMergingUploadHelper.upload(
                getFileMergingUploadRequest(s3FileMergingRequest, s3KeysInfo));
        deleteOldFiles(s3FileMergingRequest, s3KeysInfo);
        return getS3FileMergingResponse(fileMergingUploadResponse.getBucketName(),
                fileMergingUploadResponse.getS3Key());
    }

    private FileMergingUploadRequest getFileMergingUploadRequest(
        final S3FileMergingRequest s3FileMergingRequest, final List<S3KeyInfo> s3KeysInfo) {
        return FileMergingUploadRequest.builder()
            .sourceBucketName(s3FileMergingRequest.getSourceBucketName())
            .destinationBucketName(s3FileMergingRequest.getDestinationBucketName())
            .s3KeysInfo(s3KeysInfo)
            .mergedFileS3KeyPrefix(s3FileMergingRequest.getMergedFileS3KeyPrefix())
            .shouldManageHeaders(s3FileMergingRequest.getShouldManageHeaders()).build();
    }

    private void deleteOldFiles(final S3FileMergingRequest s3FileMergingRequest,
        final List<S3KeyInfo> s3KeysInfo) {
        try {
            if (s3FileMergingRequest.getDeleteAfterMerge()) {
                List<KeyVersion> keyVersions =
                    s3KeysInfo.stream().map(s3KeyInfo -> new KeyVersion(s3KeyInfo.getKeyName()))
                        .collect(Collectors.toList());
                DeleteObjectsRequest deleteObjectRequest =
                    new DeleteObjectsRequest(s3FileMergingRequest.getSourceBucketName());
                deleteObjectRequest.setKeys(keyVersions);
                amazonS3.deleteObjects(deleteObjectRequest);
            }
        } catch (AmazonServiceException e) {
            log.warn(String.format("Failed to delete old files for request %s",
                s3FileMergingRequest), e);
        }
    }

    private List<S3KeyInfo> getS3KeysInfo(final S3FileMergingRequest s3FileMergingRequest)
        throws FileMergingException {
        if (s3FileMergingRequest.getUseS3FilePrefix()) {
            return getS3KeysInfoUsingPrefix(s3FileMergingRequest.getSourceBucketName(),
                s3FileMergingRequest.getS3FilePrefix());
        } else {
            return getS3KeysInfoUsingS3Keys(s3FileMergingRequest.getSourceBucketName(),
                s3FileMergingRequest.getS3Keys());
        }
    }

    private List<S3KeyInfo> getS3KeysInfoUsingS3Keys(final String bucketName,
        final List<String> s3Keys) {
        try {
            ForkJoinPool threadPool = new ForkJoinPool(MAXIMUM_THREAD_COUNT);
            return threadPool.submit(() -> s3Keys.stream().parallel()
                .map(s3Key -> new S3KeyInfo(s3Key, amazonS3.getObjectMetadata(bucketName,
                    s3Key).getContentLength())).collect(Collectors.toList())).get();
        } catch (InterruptedException | ExecutionException e) {
            String errorMessage = "Failed to get the S3 keys info";
            log.error(errorMessage, e);
            throw new FileMergingException(errorMessage, e);
        }
    }

    private List<S3KeyInfo> getS3KeysInfoUsingPrefix(final String bucketName,
        final String s3FilePrefix) throws FileMergingException {
        try {
            List<S3KeyInfo> s3KeysInfo = new ArrayList<>();
            ListObjectsV2Result listObjectV2Result = amazonS3
                .listObjectsV2(bucketName, s3FilePrefix);
            List<S3ObjectSummary> objectSummaries = listObjectV2Result.getObjectSummaries();
            s3KeysInfo.addAll(objectSummaries.stream().map(
                s3ObjectSummary -> new S3KeyInfo(s3ObjectSummary.getKey(),
                    s3ObjectSummary.getSize())).collect(Collectors.toList()));
            while (listObjectV2Result.isTruncated()) {
                String nextContinuationToken = listObjectV2Result.getNextContinuationToken();
                listObjectV2Result = amazonS3.listObjectsV2(
                    new ListObjectsV2Request().withBucketName(bucketName).withPrefix(s3FilePrefix)
                        .withContinuationToken(nextContinuationToken));
                objectSummaries = listObjectV2Result.getObjectSummaries();
                s3KeysInfo.addAll(objectSummaries.stream().map(
                    s3ObjectSummary -> new S3KeyInfo(s3ObjectSummary.getKey(),
                        s3ObjectSummary.getSize())).collect(Collectors.toList()));
            }
            return s3KeysInfo.stream().filter(s3KeyInfo -> s3KeyInfo.getSize() != 0)
                .collect(Collectors.toList());
        } catch (AmazonServiceException e) {
            String errorMessage = String.format(
                "Failed to get the files for prefix %s from bucket %s", s3FilePrefix, bucketName);
            log.error(errorMessage, e);
            throw new FileMergingException(errorMessage, e);
        }
    }
}
