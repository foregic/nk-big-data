package com.test.nkhadoop.mr.entity;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KeyWord implements Writable {
    private String word;
    private int num;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(word);
        dataOutput.writeInt(num);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        word = dataInput.readUTF();
        num = dataInput.readInt();
    }

    @Override
    public String toString() {
        return word+","+num;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public KeyWord() {
    }

    public KeyWord(String word, int num) {
        this.word = word;
        this.num = num;
    }

}
