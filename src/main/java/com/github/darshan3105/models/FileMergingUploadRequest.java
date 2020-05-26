package com.github.darshan3105.models;

import java.util.List;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
public class FileMergingUploadRequest {

    @NonNull
    private String sourceBucketName;
    @NonNull
    private String destinationBucketName;
    @NonNull
    private List<S3KeyInfo> s3KeysInfo;
    @NonNull
    private String mergedFileS3KeyPrefix;
    @NonNull
    private boolean shouldManageHeaders;
}
