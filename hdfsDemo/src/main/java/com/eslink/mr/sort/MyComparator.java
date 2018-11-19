package com.eslink.mr.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyComparator extends WritableComparator {

    protected MyComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable k1, WritableComparable k2) {
        String[] a1 = k1.toString().split("\t");
        String[] a2 = k2.toString().split("\t");
        //如果种类字段相同，则比较价格字段
        if (a1[2].equals(a2[2])) {
            //如果价格也相同，如果返回0，则认为是相同的书；所以需要进一步比较书名
            if (a1[1].equals(a2[1])) {
                return a1[1].compareTo(a1[3]);
            } else {
                return a1[1].compareTo(a2[1]);
            }
        } else {
            return a1[2].compareTo(a2[2]);
        }
    }
}