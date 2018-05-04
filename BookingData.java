package com.gojek.bi.bookingproject;

import org.apache.beam.runners.direct.repackaged.javax.annotation.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

/**
 *
 * @author Arinda
 */
@DefaultCoder(AvroCoder.class)
public class BookingData {
    @Nullable private String service_area_name;
    @Nullable private String payment_type;
    @Nullable private String status;
    @Nullable private String order_number;
    @Nullable private String service_type;
    @Nullable private String driver_id;
    @Nullable private String customer_id;
    @Nullable private String event_timestamp;

    public BookingData() {
    }
    
    public BookingData(String service_area_name, String payment_type, String status, String order_number, String service_type, String driver_id, String customer_id, String event_timestamp) {
        this.service_area_name = service_area_name;
        this.payment_type = payment_type;
        this.status = status;
        this.order_number = order_number;
        this.service_type = service_type;
        this.driver_id = driver_id;
        this.customer_id = customer_id;
        this.event_timestamp = event_timestamp;
    }

    
    public String getService_area_name() {
        return service_area_name;
    }

    public void setService_area_name(String service_area_name) {
        this.service_area_name = service_area_name;
    }

    public String getPayment_type() {
        return payment_type;
    }

    public void setPayment_type(String payment_type) {
        this.payment_type = payment_type;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getOrder_number() {
        return order_number;
    }

    public void setOrder_number(String order_number) {
        this.order_number = order_number;
    }

    public String getService_type() {
        return service_type;
    }

    public void setService_type(String service_type) {
        this.service_type = service_type;
    }

    public String getDriver_id() {
        return driver_id;
    }

    public void setDriver_id(String driver_id) {
        this.driver_id = driver_id;
    }

    public String getCustomer_id() {
        return customer_id;
    }

    public void setCustomer_id(String customer_id) {
        this.customer_id = customer_id;
    }

    public String getEvent_timestamp() {
        return event_timestamp;
    }

    public void setEvent_timestamp(String event_timestamp) {
        this.event_timestamp = event_timestamp;
    }
}
