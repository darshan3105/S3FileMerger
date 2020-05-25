package com.github.darshan3105.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

/**
 * Model class for storing the key name and size of the S3 object.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public final class S3KeyInfo {

    @NonNull
    private String keyName;
    /**
     * Size of the key in bytes.
     */
    @NonNull
    private Long size;
}
