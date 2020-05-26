package com.github.darshan3105.models;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
public class FileMergingUploadResponse {

    @NonNull
    private String bucketName;
    @NonNull
    private String s3Key;
}
