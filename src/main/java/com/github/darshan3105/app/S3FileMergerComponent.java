package com.github.darshan3105.app;

import javax.inject.Singleton;

import com.github.darshan3105.provider.AWSResourceModule;

import dagger.Component;

@Singleton
@Component(modules = {AWSResourceModule.class})
public interface S3FileMergerComponent {

    S3FileMergerController getS3FileMerger();
}
