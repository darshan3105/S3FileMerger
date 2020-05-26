package com.github.darshan3105.models;

import java.util.List;

import com.amazonaws.services.s3.model.PartETag;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
public final class MultiPartUploadResponse {

    private int nextPartNumber;
    @NonNull
    private List<PartETag> partETags;
}
