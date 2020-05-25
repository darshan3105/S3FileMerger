package com.github.darshan3105.models;

import java.util.List;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
public class S3Parts {

    @NonNull
    List<S3KeyInfo> keysAboveMaximumPartSize;
    @NonNull
    List<List<S3KeyInfo>> keysBelowMaximumPartSize;
}
