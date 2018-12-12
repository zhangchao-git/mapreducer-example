package cn.zoua.hadoop.mr.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    private String orderId;

    private Double price;

    public OrderBean() {
    }

    public OrderBean(String orderId, Double price) {
        this.orderId = orderId;
        this.price = price;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.price = in.readDouble();
    }

    @Override
    public int compareTo(OrderBean o) {
        //两次排序
        //按照id排序
        int result = this.orderId.compareTo(o.getOrderId());
        if (result == 0) {
            // 2 再按金额排序（从大到小）
            result = this.price > o.getPrice() ? 1 : -1;
        }
        return result;
    }

    @Override
    public String toString() {
        return orderId + "\t" + price;
    }
}
