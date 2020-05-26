package com.github.darshan3105.models;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
public final class MultiPartUploadRequest {

    @NonNull
    private String sourceBucketName;
    @NonNull
    private String destinationBucketName;
    @NonNull
    private String uploadId;
    @NonNull
    private S3Parts s3Parts;
    private int startPartNumber;
    private boolean shouldManageHeaders;
}
