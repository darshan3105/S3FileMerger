package com.github.darshan3105.utils;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

public class S3FileMergingUtilsTest {

    private static final String NEW_LINE = "\n";

    @Test(expected = NullPointerException.class)
    public void testMergeS3Files_ShouldThrowNullPointerException_ForNullObjectsContent() {
        S3FileMergingUtils.mergeS3Files(null, true);
    }

    @Test
    public void testMergeS3Files_ShouldRunSuccessfully_WithOnlyHeadersOfFirstFileRetained()
        throws IOException {
        List<String> obj1 = getS3ObjectContent("src/test/java/resources/SampleS3File.csv");
        List<String> obj2 = getS3ObjectContent("src/test/java/resources/SampleS3File.csv");
        List<List<String>> s3ObjectsContent = new ArrayList<List<String>>() {{
            add(obj1);
            add(obj2);
        }};
        String output = S3FileMergingUtils.mergeS3Files(s3ObjectsContent, true);
        List<String> lines = Arrays.asList(output.split(NEW_LINE));
        assertEquals(4, lines.size());
    }

    @Test
    public void testMergeS3Files_ShouldRunSuccessfully_WithAllHeadersRetained() throws IOException {
        List<String> obj1 = getS3ObjectContent("src/test/java/resources/SampleS3File.csv");
        List<String> obj2 = getS3ObjectContent("src/test/java/resources/SampleS3File.csv");
        List<List<String>> s3ObjectsContent = new ArrayList<List<String>>() {{
            add(obj1);
            add(obj2);
        }};
        String output = S3FileMergingUtils.mergeS3Files(s3ObjectsContent, false);
        List<String> lines = Arrays.asList(output.split(NEW_LINE));
        assertEquals(6, lines.size());
    }

    private List<String> getS3ObjectContent(String sampleS3FilePath) throws IOException {
        InputStream stream = FileUtils.openInputStream(new File(sampleS3FilePath));
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream,
            Charset.defaultCharset()));
        return reader.lines().collect(Collectors.toList());
    }
}
