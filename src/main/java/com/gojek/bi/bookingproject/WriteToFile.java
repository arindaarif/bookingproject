package com.gojek.bi.bookingproject;

import static com.google.common.base.MoreObjects.firstNonNull;

import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileBasedSink.OutputFileHints;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 *
 * @author Arinda
 * 
 */

//This class is used to write a file for each window
public class WriteToFile extends PTransform<PCollection<String>, PDone> {
    private static final DateTimeFormatter FORMATTER = ISODateTimeFormat.hourMinute();
    private String filenamePrefix;
    private static String windowStart;
    private static String windowEnd;

    public WriteToFile(String filenamePrefix) {
        this.filenamePrefix = filenamePrefix;
    }

    //Using TextIO to write into csv files
    @Override
    public PDone expand(PCollection<String> input) {
        ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(filenamePrefix);
        
        TextIO.Write write = TextIO
                .write()
                .to(new PerWindowFiles(resource))
                .withTempDirectory(resource.getCurrentDirectory())
                .withWindowedWrites()
                .withHeader("SERVICE_AREA,PAYMENT,STATUS,TOTAL_BOOKING");
        write = write.withNumShards(1);
        
        return input.
                apply(write);
    }

    public static class PerWindowFiles extends FilenamePolicy {

        private final ResourceId baseFilename;

        public PerWindowFiles(ResourceId baseFilename) {
            this.baseFilename = baseFilename;
        }

        public String filenamePrefixForWindow(IntervalWindow window) {
            windowStart = FORMATTER.print(window.start());
            windowEnd = FORMATTER.print(window.end());
            String prefix = baseFilename.isDirectory() ? "" : firstNonNull(baseFilename.getFilename(), "");
            return String.format("%s_%s_%s", prefix, windowStart, windowEnd);
            
        }
        
        //Naming each window
        @Override
        public ResourceId windowedFilename(int shardNumber,
                                         int numShards,
                                         BoundedWindow window,
                                         PaneInfo paneInfo,
                                         OutputFileHints outputFileHints) {
            IntervalWindow intervalWindow = (IntervalWindow) window;
            String temp_prefix=filenamePrefixForWindow(intervalWindow);
            
            //Since each file named with start & end of each window (time format), I have to replace ':' into '-'
            temp_prefix = temp_prefix.replace(":", "-");
            String filename = String.format("%s%s",temp_prefix,".csv");
            return baseFilename
                .getCurrentDirectory()
                .resolve(filename, StandardResolveOptions.RESOLVE_FILE);
        }

        @Override
        public ResourceId unwindowedFilename(int shardNumber, int numShards, OutputFileHints outputFileHints) {
            throw new UnsupportedOperationException("Unsupported.");
        }
    }
}
