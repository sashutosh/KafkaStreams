package com.example.serdes;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {

    private Gson gson;

    private Class<T> destinationClass;
    private Type reflectionTypeToken;

    public JsonDeserializer(){

    }

    public JsonDeserializer(Class<T> destinationClass) {
        this.destinationClass = destinationClass;
        init();
    }

    public JsonDeserializer(Type reflectionTypeToken) {
        this.reflectionTypeToken = reflectionTypeToken;
        init();
    }

    private void init(){
        GsonBuilder builder = new GsonBuilder();
        gson = builder.create();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if(destinationClass == null) {
            destinationClass = (Class<T>) configs.get("serializedClass");
        }
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        try {
            Type type = destinationClass != null ? destinationClass : reflectionTypeToken;
            return gson.fromJson(new String(bytes, StandardCharsets.UTF_8), type);
        } catch (Exception e) {
            throw new SerializationException("Error deserializing message", e);
        }
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        return deserialize(topic,data);
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
