package com.atguigu.mr.grouping;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    private String orderId;
    private String productId;
    private double price;

    @Override
    public String toString() {
        return orderId + "\t" + productId + "\t" + price;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    /**
     * 排序逻辑：先按照订单排序，订单相同按照价格降序排列
     * @param o
     * @return
     */
    @Override
    public int compareTo(OrderBean o) {
        int compare = this.orderId.compareTo(o.orderId);
        if (compare != 0) {
            return compare;
        } else {
            return Double.compare(o.price, this.price);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeUTF(productId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.productId = in.readUTF();
        this.price = in.readDouble();
    }
}
