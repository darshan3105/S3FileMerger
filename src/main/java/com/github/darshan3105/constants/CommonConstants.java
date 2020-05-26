package com.github.darshan3105.constants;

import org.apache.commons.io.FileUtils;

public final class CommonConstants {
    
    private CommonConstants(){
        
    }

    public static final String NEW_LINE = "\n";
    public static final long MB = FileUtils.ONE_MB;
    public static final long MAXIMUM_PART_SIZE = 100 * MB;
    public static final long MINIMUM_PART_SIZE = 5 * MB;
}
