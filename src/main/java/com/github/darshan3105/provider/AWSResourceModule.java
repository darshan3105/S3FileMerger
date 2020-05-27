package com.github.darshan3105.provider;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import dagger.Module;
import dagger.Provides;

@Module
public class AWSResourceModule {


    /**
     * Provides Amazon S3 client.
     *
     * @return AmazonS3 {@link AmazonS3}.
     */
    @Provides
    AmazonS3 provideAmazonS3() {
        return AmazonS3ClientBuilder.defaultClient();
    }
}
