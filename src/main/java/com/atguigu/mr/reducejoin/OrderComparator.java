package com.atguigu.mr.reducejoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组比较器，按照Order对象的Pid分组
 */
public class OrderComparator extends WritableComparator {

    protected OrderComparator() {
        super(OrderBean.class, true);

    }

    /**
     * 按照PID比较a和b
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean oa = (OrderBean) a;
        OrderBean ob = (OrderBean) b;
        return oa.getPid().compareTo(ob.getPid());
    }
}
