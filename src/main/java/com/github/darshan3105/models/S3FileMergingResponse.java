package com.github.darshan3105.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

/**
 * Model class for S3 file merging response.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public final class S3FileMergingResponse {

    @NonNull
    private String bucketName;
    @NonNull
    private String resultFilePath;
}
