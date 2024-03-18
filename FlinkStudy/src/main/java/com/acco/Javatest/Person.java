package com.acco.Javatest;

import org.apache.yetus.audience.InterfaceAudience;

import javax.xml.ws.soap.Addressing;

/**
 * ClassName: Person
 * Description: None
 * Package: com.acco.Javatest
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-02 14:04
 */
public class Person implements Cloneable{
    int age;
    String name;

    public Person(){}

    @Override
    protected Person clone() throws CloneNotSupportedException {
        return (Person) super.clone();
    }

    ;
    public Person(int age){
        this.age =age;
    }
}
