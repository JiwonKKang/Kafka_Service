package com.example.kafkaproducer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class StreamService {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Autowired
    public void buildPipeline(StreamsBuilder sb) {
        KStream<String, String> myStream = sb.stream("fastCampus", Consumed.with(STRING_SERDE, STRING_SERDE));
        //일단 consume을 한뒤에 받은 데이터를 프로세싱하여 다른토픽에 Publish
        myStream.print(Printed.toSysOut());
        myStream.filter((key, value) -> value.contains("freeClass")).to("freeClassList");


    }

}
