package com.github.darshan3105.utils;

import static com.github.darshan3105.constants.CommonConstants.MAXIMUM_PART_SIZE;
import static com.github.darshan3105.constants.CommonConstants.MINIMUM_PART_SIZE;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.FilenameUtils;

import com.github.darshan3105.exceptions.FileMergingException;
import com.github.darshan3105.models.S3KeyInfo;
import com.github.darshan3105.models.S3Parts;

import lombok.extern.log4j.Log4j2;

@Log4j2
public final class S3Utils {

    private static final boolean VALID = true;
    private static final boolean INVALID = false;
    private static final boolean USE_MULTIPART_UPLOAD = true;
    private static final boolean DO_NOT_USE_MULTIPART_UPLOAD = false;

    private S3Utils() {

    }

    /**
     * @param s3KeysInfo of type {@link List <S3KeyInfo>}
     * @return a list of S3Key chunks with each chunk of size not greater than MAXIMUM_CHUNK_SIZE.
     */
    public static S3Parts chunkBySize(final List<S3KeyInfo> s3KeysInfo) {
        if (validKeys(s3KeysInfo)) {
            List<S3KeyInfo> keysAboveMaximumPartSize = getS3KeysAboveMaximumPartSize(s3KeysInfo);
            List<S3KeyInfo> keysBelowMaximumPartSize = getS3KeysBelowMaximumPartSize(s3KeysInfo);
            keysBelowMaximumPartSize.sort(Comparator.comparing(S3KeyInfo::getSize).reversed());
            List<List<S3KeyInfo>> chunks = new ArrayList<>();
            long currentSize = 0L;
            List<S3KeyInfo> currentList = new ArrayList<>();
            for (S3KeyInfo s3KeyInfo : keysBelowMaximumPartSize) {
                currentSize += s3KeyInfo.getSize();
                if (currentSize <= MAXIMUM_PART_SIZE) {
                    currentList.add(s3KeyInfo);
                } else {
                    chunks.add(currentList);
                    currentList = new ArrayList<>();
                    currentList.add(s3KeyInfo);
                    currentSize = s3KeyInfo.getSize();
                }
            }
            if (currentSize != 0L) {
                chunks.add(currentList);
            }
            return S3Parts.builder().keysAboveMaximumPartSize(keysAboveMaximumPartSize)
                .keysBelowMaximumPartSize(chunks).build();
        } else {
            throw new FileMergingException(String.format("Files should have size less than %d",
                MAXIMUM_PART_SIZE));
        }
    }

    private static List<S3KeyInfo> getS3KeysBelowMaximumPartSize(final List<S3KeyInfo> s3KeysInfo) {
        return s3KeysInfo.stream().filter(s3KeyInfo -> s3KeyInfo.getSize() <= MAXIMUM_PART_SIZE)
            .collect(Collectors.toList());
    }
    private static List<S3KeyInfo> getS3KeysAboveMaximumPartSize(final List<S3KeyInfo> s3KeysInfo) {
        return s3KeysInfo.stream().filter(s3KeyInfo -> s3KeyInfo.getSize() > MAXIMUM_PART_SIZE)
            .collect(Collectors.toList());
    }

    /**
     * @param s3KeysInfo of type {@link List<S3KeyInfo>}
     * @return a boolean telling whether to perform multipart upload for the set of s3 keys.
     */
    public static boolean shouldUseMultiPartUpload(final List<S3KeyInfo> s3KeysInfo) {
        final long totalSize = s3KeysInfo.stream().mapToLong(S3KeyInfo::getSize).sum();
        if (totalSize < MINIMUM_PART_SIZE) {
            return DO_NOT_USE_MULTIPART_UPLOAD;
        } else {
            return USE_MULTIPART_UPLOAD;
        }
    }

    /**
     * @param fileName name of the file.
     * @return the file extension.
     */
    public static String getFileFormat(final String fileName) {
        return FilenameUtils.getExtension(fileName);
    }

    private static boolean validKeys(final List<S3KeyInfo> s3KeysInfo) {
        return isFileFormatConsistent(s3KeysInfo);
    }

    private static boolean isFileFormatConsistent(final List<S3KeyInfo> s3KeysInfo)
        throws FileMergingException {
        final String fileFormat = getFileFormat(s3KeysInfo.get(0).getKeyName());
        for (S3KeyInfo s3KeyInfo : s3KeysInfo) {
            if (!fileFormat.equals(getFileFormat(s3KeyInfo.getKeyName()))) {
                log.error(String.format("Files %s are not of the same format", s3KeysInfo));
                return INVALID;
            }
        }
        return VALID;
    }
}
