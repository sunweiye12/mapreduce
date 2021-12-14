package com.atguigu.mr.compare;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FlowComparator extends WritableComparator {

    protected FlowComparator() {
        super(FlowBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        FlowBean fa = (FlowBean) a;
        FlowBean fb = (FlowBean) b;

        return Long.compare(fb.getSumFlow(), fa.getSumFlow());
    }
}
