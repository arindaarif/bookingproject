package com.gojek.bi.bookingproject;

import com.gojek.bi.bookingproject.BookingReport.MappingEvent;
import com.gojek.bi.bookingproject.BookingReport.ParseEvent;
import com.gojek.bi.bookingproject.BookingReport.SumUpEvent;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Rule;
import org.junit.experimental.categories.Category;

/**
 *
 * @author Arinda
 */
public class BookingReportTest {
    
    public BookingReportTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Rule
    public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
    
    static final String json_string = "[{\"order_number\":\"AP-1\",\"service_type\":\"GO_RIDE\",\"driver_id\":\"driver-123\",\"customer_id\":\"customer-456\",\"service_area_name\":\"JAKARTA\",\"payment_type\":\"GO_PAY\",\"status\":\"COMPLETED\",\"event_timestamp\":\"2018-03-29T10:00:08.000Z\"},{\"order_number\":\"AP-2\",\"service_type\":\"GO_FOOD\",\"driver_id\":\"driver-124\",\"customer_id\":\"customer-457\",\"service_area_name\":\"JAKARTA\",\"payment_type\":\"GO_PAY\",\"status\":\"COMPLETED\",\"event_timestamp\":\"2018-03-29T10:00:09.000Z\"},{\"order_number\":\"AP-3\",\"service_type\":\"GO_MART\",\"driver_id\":\"driver-125\",\"customer_id\":\"customer-458\",\"service_area_name\":\"JAKARTA\",\"payment_type\":\"GO_CASH\",\"status\":\"COMPLETED\",\"event_timestamp\":\"2018-03-29T10:00:13.000Z\"}]";
    static final String[] final_str = new String[] {"JAKARTA,GO_PAY,COMPLETED,2\r", "JAKARTA,GO_CASH,COMPLETED,1\r"};
    
    //Class to assert ParseEvent Transform, whether the input data (string in json format) successfully parsed into BookingData object or not
    private static class CheckerParseEvent implements SerializableFunction<Iterable<BookingData>, Void> {
        @Override
        public Void apply(Iterable<BookingData> input) {
            Integer count_gopay = 0;
            Integer count_gocash = 0;
            for (BookingData bd : input) {
                if(bd.getPayment_type().equals("GO_PAY")){
                    count_gopay++;
                }else if(bd.getPayment_type().equals("GO_CASH")){
                    count_gocash++;
                }
            }
            assertEquals(2, count_gopay.longValue());
            assertEquals(1, count_gocash.longValue());
            return null;
        }
    }
    
    //Class to assert the Mapping Transform, whether the input data (collections of BookingData) correctly transformed into Pairs
    private static class CheckerKV implements SerializableFunction<Iterable<KV<String, Integer>>, Void> {
        @Override
        public Void apply(Iterable<KV<String, Integer>> input) {
            Integer count = 0;
            for (KV<String, Integer> kv : input) {
                count++;
            }
            assertEquals(3, count.longValue());
            return null;
        }
    }
    
    //Class to assert the GroupByKey Transform, whether it is successfully grouping by key and produce the desired output or not
    private static class CheckerKVGrouped implements SerializableFunction<Iterable<KV<String, Iterable<Integer>>>, Void> {
        @Override
        public Void apply(Iterable<KV<String, Iterable<Integer>>> input) {
            Map map = new HashMap();
            Integer count = 0;
            for (KV<String, Iterable<Integer>> kv : input) {
                for(Integer val : kv.getValue()){
                    count++;
                }
                map.put(kv.getKey(), count);
                count = 0;
            }
            assertEquals(map.get("JAKARTA,GO_PAY,COMPLETED"), 2);
            assertEquals(map.get("JAKARTA,GO_CASH,COMPLETED"), 1);
            return null;
        }
    }
    
    @Test
    @Category(ValidatesRunner.class)
    public void testCountWords() throws Exception {
        //json_string has been defined above (static final string)
        PCollection<String> input = p.apply(Create.of(json_string).withCoder(StringUtf8Coder.of()));
        
        PCollection<BookingData> output1 = input.apply("ParseBookingTest", ParDo.of(new ParseEvent()));
        PAssert.that(output1).satisfies(new CheckerParseEvent());
      
        PCollection<KV<String, Integer>> output2 = output1.apply("MappingTest", ParDo.of(new MappingEvent()));
        PAssert.that(output2).satisfies(new CheckerKV());

        PCollection<KV<String, Iterable<Integer>>> output3 = output2.apply(GroupByKey.<String, Integer>create());       
        PAssert.that(output3).satisfies(new CheckerKVGrouped());
       
        PCollection<String> output4 = output3.apply("SumUpByKeyTest", ParDo.of(new SumUpEvent()));       
        PAssert.that(output4).containsInAnyOrder(final_str); //final_str has been defined above (static final array of strings)
       
        p.run().waitUntilFinish();
    }    
}
