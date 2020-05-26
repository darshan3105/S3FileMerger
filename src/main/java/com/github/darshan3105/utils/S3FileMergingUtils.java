package com.github.darshan3105.utils;

import static com.github.darshan3105.constants.CommonConstants.NEW_LINE;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.model.PartETag;
import com.github.darshan3105.constants.CommonConstants;
import com.github.darshan3105.models.FileMergingUploadResponse;
import com.github.darshan3105.models.MultiPartUploadRequest;
import com.github.darshan3105.models.MultiPartUploadResponse;
import com.github.darshan3105.models.S3FileMergingResponse;
import com.github.darshan3105.models.S3Parts;

import lombok.NonNull;
import lombok.extern.log4j.Log4j2;

/**
 * Util for merging the content of S3 files.
 */
@Log4j2
public final class S3FileMergingUtils {

    private S3FileMergingUtils() {

    }

    /**
     * @param s3ObjectsContent a list of list of strings, containing the content of the S3 objects.
     * @param shouldRemoveHeader a boolean specifying if the headers of the files should be
     * managed or not.
     * @return merged S3 object content.
     */
    public static String mergeS3Files(@NonNull final List<List<String>> s3ObjectsContent,
        final boolean shouldRemoveHeader) {
        return new StringBuilder().append(
            s3ObjectsContent.stream().map(s3ObjectContent -> getContentAsString(s3ObjectContent,
                shouldRemoveHeader)).collect(Collectors.joining(NEW_LINE))).toString();
    }

    /**
     * @param s3ObjectContent a list of strings containing the content of S3Object
     * @return the header of the S3Object.
     */
    public static String getFileHeader(final List<String> s3ObjectContent) {
        return new StringBuilder().append(s3ObjectContent.get(0)).append(NEW_LINE).toString();
    }

    private static String getContentAsString(final List<String> s3KeyContent,
        final boolean shouldRemoveHeader) {
        if (shouldRemoveHeader) {
            return s3KeyContent.stream().skip(1).collect(Collectors.joining(NEW_LINE));
        } else {
            return s3KeyContent.stream().collect(Collectors.joining(NEW_LINE));
        }
    }

    /**
     * @param mergedFileS3KeyPrefix prefix of the merged S3 file
     * @param fileFormat format of the file
     * @return the S3 key of merged file
     */
    public static String generateMergedFileS3Key(final String mergedFileS3KeyPrefix,
        final String fileFormat) {
        return new StringBuilder().append(mergedFileS3KeyPrefix)
            .append(UUID.randomUUID().toString()).append(".").append(fileFormat).toString();
    }

    /**
     * @param bucketName S3 bucket name
     * @param resultFilePath S3 key of the merged file
     * @return an instance of {@link S3FileMergingResponse}
     */
    public static S3FileMergingResponse getS3FileMergingResponse(final String bucketName,
        final String resultFilePath) {
        return S3FileMergingResponse.builder().bucketName(bucketName)
            .resultFilePath(resultFilePath).build();
    }

    public static MultiPartUploadRequest generateMultiPartUploadRequest(
        final String sourceBucketName, final String destinationBucketName, final String uploadId,
        final S3Parts s3Parts, final int startPartNumber, final boolean shouldManageHeaders) {
        return MultiPartUploadRequest.builder().sourceBucketName(sourceBucketName)
            .destinationBucketName(destinationBucketName).uploadId(uploadId).s3Parts(s3Parts)
            .startPartNumber(startPartNumber).shouldManageHeaders(shouldManageHeaders).build();
    }

    public static MultiPartUploadResponse generateMultiPartUploadResponse(final int nextPartNumber,
        final List<PartETag> partETags) {
        return MultiPartUploadResponse.builder().nextPartNumber(nextPartNumber).partETags(partETags)
            .build();
    }

    public static FileMergingUploadResponse generateFileMergingUploadResponse(
        final String bucketName, final String s3Key) {
        return FileMergingUploadResponse.builder().bucketName(bucketName).s3Key(s3Key).build();
    }
}
