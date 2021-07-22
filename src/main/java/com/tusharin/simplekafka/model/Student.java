package com.tusharin.simplekafka.model;

import org.apache.kafka.common.serialization.Serializer;

public class Student<iType, vType>  {
    private iType id;
    private vType name;

    public Student(iType id, vType name) {
        this.id = id;
        this.name = name;
    }

    @Override
    public String toString() {
        return "Student{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
    }
}
