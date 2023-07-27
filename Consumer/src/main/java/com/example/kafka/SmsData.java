package com.example.kafka;

import java.util.List;

public class SmsData {
    private List<SmsEntry> data;

    public List<SmsEntry> getData() {
        return data;
    }

    public void setData(List<SmsEntry> data) {
        this.data = data;
    }
}
