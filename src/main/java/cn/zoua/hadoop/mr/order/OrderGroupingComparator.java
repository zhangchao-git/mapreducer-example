package cn.zoua.hadoop.mr.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator {

    protected OrderGroupingComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        OrderBean abean = (OrderBean) a;
        OrderBean bbean = (OrderBean) b;

        // 将orderId相同的bean都视为一组
        return abean.getOrderId().compareTo(bbean.getOrderId());
    }
}
