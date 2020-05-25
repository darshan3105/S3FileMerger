package com.github.darshan3105.models;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

/**
 * Model class for S3 file merging request.
 */
@Data
@AllArgsConstructor
@Builder
public final class S3FileMergingRequest {

    @NonNull
    private String sourceBucketName;
    @NonNull
    private String destinationBucketName;
    private List<String> s3Keys;
    private String s3FilePrefix;
    /**
     * Prefix of the new file that is a concatenation of the contents of the given files.
     */
    private String mergedFileS3KeyPrefix;
    /**
     * Should be set to true if the lambda should use the s3 file prefix to get the files from S3
     * and then merge those files.
     */
    @NonNull
    private Boolean useS3FilePrefix;
    /**
     * Should be set to true if the older files should be deleted after merging.
     */
    @NonNull
    private Boolean deleteAfterMerge;
    /**
     * This option is relevant for files in .csv or .tsv format.
     * If set to false, the files will be merged as they are line after line.
     * If set to true, only the header of the first file will be retained, and headers of other
     * files will be dropped. If this option is used, the assumption is that all the files should
     * have the same headers and in the same order.
     */
    @NonNull
    private Boolean shouldManageHeaders;
}
