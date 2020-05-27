package com.github.darshan3105.helpers;

import java.nio.charset.Charset;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.io.IOUtils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

import lombok.NonNull;

public class S3Helper {

    private final AmazonS3 amazonS3;

    @Inject
    public S3Helper(@NonNull final  AmazonS3 amazonS3) {
        this.amazonS3 = amazonS3;
    }

    void abortMultiPartUpload(final String bucketName, final String s3Key,
        final String uploadId) {
        AbortMultipartUploadRequest abortMultipartUploadRequest =
            new AbortMultipartUploadRequest(bucketName, s3Key, uploadId);
        amazonS3.abortMultipartUpload(abortMultipartUploadRequest);
    }

    void completeMultiPartUpload(final String bucketName, final String s3Key,
        final String uploadId, final List<PartETag> partETags) {
        CompleteMultipartUploadRequest completeMultipartUploadRequest =
            new CompleteMultipartUploadRequest().withUploadId(uploadId)
                .withBucketName(bucketName)
                .withKey(s3Key).withPartETags(partETags);
        amazonS3.completeMultipartUpload(completeMultipartUploadRequest);
    }

    PartETag uploadPart(final String bucketName, final String s3Key,
        final String uploadId, final int partNumber, final String content) {
        UploadPartRequest uploadPartRequest =
            new UploadPartRequest().withUploadId(uploadId).withPartNumber(partNumber)
                .withBucketName(bucketName).withKey(s3Key).withInputStream(
                IOUtils.toInputStream(content, Charset.defaultCharset()))
                .withPartSize(content.getBytes(Charset.defaultCharset()).length);
        UploadPartResult uploadPartResult = amazonS3.uploadPart(uploadPartRequest);
        return uploadPartResult.getPartETag();
    }

    String startMultiPartUpload(final String bucketName, final String s3Key) {
        InitiateMultipartUploadRequest initiateMultipartUploadRequest =
            new InitiateMultipartUploadRequest(bucketName, s3Key);
        InitiateMultipartUploadResult initiateMultipartUploadResult =
            amazonS3.initiateMultipartUpload(initiateMultipartUploadRequest);
        return initiateMultipartUploadResult.getUploadId();
    }

    void uploadObject(final String bucketName, final String s3Key,
        final String content) {
        amazonS3.putObject(bucketName, s3Key, content);
    }

    S3Object getS3Object(final String bucketName, final String s3Key) {
        return amazonS3.getObject(bucketName, s3Key);
    }
}
