package com.gojek.bi.bookingproject;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 *
 * @author Arinda
 */
public class BookingReport {
    
    //This class is used for ParseBooking, which transform string in json format into object of BookingData
    static class ParseEvent extends DoFn<String, BookingData> {
        @ProcessElement
        public void processElement(ProcessContext c) {
          
            String strJson = c.element();
            try {
                ObjectMapper mapper = new ObjectMapper();
                List<BookingData> listBook = mapper.readValue(strJson, new TypeReference<List<BookingData>>(){});
                for(int i=0;i<listBook.size();i++){
                    try {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
                        Date parsedDate = sdf.parse(listBook.get(i).getEvent_timestamp());
                        c.outputWithTimestamp(listBook.get(i),new Instant(parsedDate.getTime()));
                    } catch(Exception e) {
                        c.output(listBook.get(i));
                    }
                }
            } catch (Exception e) {
                
            }
        }
    }
    
    //This class is used for Mapping, which transforming objects of BookingData into pairs of key & value
    static class MappingEvent extends DoFn<BookingData, KV<String, Integer>> {

        @ProcessElement
        public void processElement(ProcessContext c) {
          
            BookingData bd = c.element();
            try {
                String temp_str = bd.getService_area_name() + "," + bd.getPayment_type() + "," + bd.getStatus();
                c.output(KV.of(temp_str, 1));
            } catch (Exception e) {
            
            }
        }
    }
    
    //This class is used for summing up the iterable values inside pair. The summed up values is the number of total booking for each key.
    static class SumUpEvent extends DoFn<KV<String, Iterable<Integer>>, String> {

        @ProcessElement
        public void processElement(ProcessContext context) {
            Integer totalBook = 0;
            String booking = context.element().getKey();
            Iterable<Integer> book = context.element().getValue();
            for (Integer amount : book) {
                totalBook += amount;
            }
            context.output(booking + "," + totalBook+"\r");
        }
    }
    
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        pipeline
            .apply(TextIO.read().from("files/input/booking_json.json"))
            .apply("ParseBooking", ParDo.of(new ParseEvent()))
            .apply(Window
                    .<BookingData>into(SlidingWindows.of(Duration.standardMinutes(60)) //Hourly window
                    .every(Duration.standardMinutes(5)))) //every 5 minutes
            .apply("Mapping", ParDo.of(new MappingEvent()))
            .apply(GroupByKey.<String, Integer>create())       
            .apply("SumUpByKey", ParDo.of(new SumUpEvent()))
            .apply(new WriteToFile("files/output/BookingReport"));
            
        pipeline.run().waitUntilFinish();
    }

}
