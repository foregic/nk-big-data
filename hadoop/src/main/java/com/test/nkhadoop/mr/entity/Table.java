package com.test.nkhadoop.mr.entity;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Table implements Writable {
    private String commodity;
    private String brand;
    private int sales;
    private int price;

    public Table() {
    }

    public Table(String commodity, String brand, int sales, int price) {
        this.commodity = commodity;
        this.brand = brand;
        this.sales = sales;
        this.price = price;
    }

    public String getCommodity() {
        return commodity;
    }

    public String getBrand() {
        return brand;
    }

    public int getSales() {
        return sales;
    }

    public int getPrice() {
        return price;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public void setSales(int sales) {
        this.sales = sales;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return commodity + '\t' + brand + '\t' + sales + '\t' + price;

//                "Table{" +
//                "commodity='" + commodity + '\'' +
//                ", brand='" + brand + '\'' +
//                ", sales=" + sales +
//                ", price=" + price +
//                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(commodity);
        dataOutput.writeUTF(brand);
        dataOutput.writeInt(sales);
        dataOutput.writeInt(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        commodity = dataInput.readUTF();
        brand = dataInput.readUTF();
        sales = dataInput.readInt();
        price = dataInput.readInt();

    }
}
