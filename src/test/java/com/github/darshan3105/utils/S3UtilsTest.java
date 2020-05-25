package com.github.darshan3105.utils;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;


import org.junit.Test;

import com.github.darshan3105.exceptions.FileMergingException;
import com.github.darshan3105.models.S3KeyInfo;
import com.github.darshan3105.models.S3Parts;

public class S3UtilsTest {

    private static final long KB = 1024;
    private static final long MB = 1024 * KB;
    private static final String DUMMY_CSV = "Dummy.csv";
    private static final String DUMMY_TSV = "Dummy.tsv";

    @Test
    public void testChunkBySize_ShouldRunSuccessfully_WithTwoChunks() {
        S3KeyInfo s3KeyInfo1 = S3KeyInfo.builder().keyName(DUMMY_CSV).size(80 * MB).build();
        S3KeyInfo s3KeyInfo2 = S3KeyInfo.builder().keyName(DUMMY_CSV).size(100 * MB).build();
        S3KeyInfo s3KeyInfo3 = S3KeyInfo.builder().keyName(DUMMY_CSV).size(20 * MB).build();
        List<S3KeyInfo> s3KeysInfo = new ArrayList<S3KeyInfo>() {{
            add(s3KeyInfo1);
            add(s3KeyInfo2);
            add(s3KeyInfo3);
        }};
        S3Parts s3Parts = S3Utils.chunkBySize(s3KeysInfo);
        assertEquals(2, s3Parts.getKeysBelowMaximumPartSize().size());
    }

    @Test
    public void testChunkBySize_ShouldRunSuccessfully_WithTwoChunksAndOneKeyMoreThanMaximumPartSize() {
        S3KeyInfo s3KeyInfo1 = S3KeyInfo.builder().keyName(DUMMY_CSV).size(80 * MB).build();
        S3KeyInfo s3KeyInfo2 = S3KeyInfo.builder().keyName(DUMMY_CSV).size(100 * MB).build();
        S3KeyInfo s3KeyInfo3 = S3KeyInfo.builder().keyName(DUMMY_CSV).size(20 * MB).build();
        S3KeyInfo s3KeyInfo4 = S3KeyInfo.builder().keyName(DUMMY_CSV).size(200 * MB).build();
        List<S3KeyInfo> s3KeysInfo = new ArrayList<S3KeyInfo>() {{
            add(s3KeyInfo1);
            add(s3KeyInfo2);
            add(s3KeyInfo3);
            add(s3KeyInfo4);
        }};
        S3Parts s3Parts = S3Utils.chunkBySize(s3KeysInfo);
        assertEquals(1,s3Parts.getKeysAboveMaximumPartSize().size());
        assertEquals(2, s3Parts.getKeysBelowMaximumPartSize().size());
    }

    @Test
    public void testChunkBySize_ShouldRunSuccessfully_WithThreeChunks() {
        S3KeyInfo s3KeyInfo1 = S3KeyInfo.builder().keyName(DUMMY_CSV).size(50 * MB).build();
        S3KeyInfo s3KeyInfo2 = S3KeyInfo.builder().keyName(DUMMY_CSV).size(100 * MB).build();
        S3KeyInfo s3KeyInfo3 = S3KeyInfo.builder().keyName(DUMMY_CSV).size(80 * MB).build();
        List<S3KeyInfo> s3KeysInfo = new ArrayList<S3KeyInfo>() {{
            add(s3KeyInfo1);
            add(s3KeyInfo2);
            add(s3KeyInfo3);
        }};
        S3Parts s3Parts = S3Utils.chunkBySize(s3KeysInfo);
        assertEquals(3, s3Parts.getKeysBelowMaximumPartSize().size());
    }

    @Test
    public void testShouldUseMultiPartUpload_ShouldReturnUseMultiPartUpload() {
        S3KeyInfo s3KeyInfo1 = S3KeyInfo.builder().keyName(DUMMY_CSV).size(40 * MB).build();
        S3KeyInfo s3KeyInfo2 = S3KeyInfo.builder().keyName(DUMMY_CSV).size(50 * MB).build();
        S3KeyInfo s3KeyInfo3 = S3KeyInfo.builder().keyName(DUMMY_CSV).size(30 * MB).build();
        List<S3KeyInfo> s3KeysInfo = new ArrayList<S3KeyInfo>() {{
            add(s3KeyInfo1);
            add(s3KeyInfo2);
            add(s3KeyInfo3);
        }};
        assertTrue(S3Utils.shouldUseMultiPartUpload(s3KeysInfo));
    }

    @Test
    public void testShouldUseMultiPartUpload_ShouldReturnDoNotUseMultiPartUpload() {
        S3KeyInfo s3KeyInfo1 = S3KeyInfo.builder().keyName(DUMMY_CSV).size(1 * MB).build();
        S3KeyInfo s3KeyInfo2 = S3KeyInfo.builder().keyName(DUMMY_CSV).size(2 * MB).build();
        S3KeyInfo s3KeyInfo3 = S3KeyInfo.builder().keyName(DUMMY_CSV).size(1 * MB).build();
        List<S3KeyInfo> s3KeysInfo = new ArrayList<S3KeyInfo>() {{
            add(s3KeyInfo1);
            add(s3KeyInfo2);
            add(s3KeyInfo3);
        }};
        assertFalse(S3Utils.shouldUseMultiPartUpload(s3KeysInfo));
    }

    @Test (expected = FileMergingException.class)
    public void testChunkBySize_ShouldThrowFileMergingException_ForInconsistentFileFormat() {
        S3KeyInfo s3KeyInfo1 = S3KeyInfo.builder().keyName(DUMMY_CSV).size(50 * MB).build();
        S3KeyInfo s3KeyInfo2 = S3KeyInfo.builder().keyName(DUMMY_TSV).size(70 * MB).build();
        S3KeyInfo s3KeyInfo3 = S3KeyInfo.builder().keyName(DUMMY_CSV).size(50 * MB).build();
        List<S3KeyInfo> s3KeysInfo = new ArrayList<S3KeyInfo>() {{
            add(s3KeyInfo1);
            add(s3KeyInfo2);
            add(s3KeyInfo3);
        }};
        S3Utils.chunkBySize(s3KeysInfo);
    }

    @Test (expected = FileMergingException.class)
    public void testChunkBySize_ShouldThrowFileMergingException_ForFileSizeGreaterThanMaxChunkSize() {
        S3KeyInfo s3KeyInfo1 = S3KeyInfo.builder().keyName(DUMMY_CSV).size(101 * MB).build();
        S3KeyInfo s3KeyInfo2 = S3KeyInfo.builder().keyName(DUMMY_TSV).size(2 * MB).build();
        S3KeyInfo s3KeyInfo3 = S3KeyInfo.builder().keyName(DUMMY_CSV).size(1 * MB).build();
        List<S3KeyInfo> s3KeysInfo = new ArrayList<S3KeyInfo>() {{
            add(s3KeyInfo1);
            add(s3KeyInfo2);
            add(s3KeyInfo3);
        }};
        S3Utils.chunkBySize(s3KeysInfo);
    }
}
