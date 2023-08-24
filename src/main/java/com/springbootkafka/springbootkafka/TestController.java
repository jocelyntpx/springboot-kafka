package com.springbootkafka.springbootkafka;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {

    private final com.springbootkafka.springbootkafka.Producer producer;

    @Autowired
    public TestController(com.springbootkafka.springbootkafka.Producer producer) {
        this.producer = producer;
    }
    @PostMapping("/publish")
    public void messageToTopic(@RequestParam("message") String message){

        this.producer.sendMessage(message);


    }
}