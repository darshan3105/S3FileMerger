package com.github.darshan3105.app;

import java.util.Objects;

import com.github.darshan3105.models.S3FileMergingRequest;
import com.github.darshan3105.models.S3FileMergingResponse;

import lombok.NonNull;

public final class S3FileMerger {

    private static S3FileMergerController s3FileMergerController = null;

    private S3FileMerger() {

    }

    public static S3FileMergingResponse mergeFiles(
        @NonNull final S3FileMergingRequest s3FileMergingRequest) {
        initialize();
        return s3FileMergerController.mergeFiles(s3FileMergingRequest);
    }

    private static void initialize() {
        if (Objects.isNull(s3FileMergerController)) {
            s3FileMergerController = DaggerS3FileMergerComponent.create().getS3FileMerger();
        }
    }
}
