package com.atguigu.mr.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TopNComparator extends WritableComparator {

    protected TopNComparator() {
        super(FlowBean.class, true);
    }

    /**
     * 将所有数据分到同一组
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return 0;
    }
}
