package com.jaeng.adhesive.core.udf;

import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * @author lizheng
 * @date 2019/7/31
 */
public class ShardingHashUdf extends AbstractUdf implements UDF3<String, Integer, Integer, Integer> {

    int hash0(String str, int bucketCount) {
        int hash = str.hashCode();
        return hash;
    }

    //用它!
    int hash1(String str, int bucketCount) {
        int hash = str.hashCode();
        return hash >>> 16;
    }

    int hash2(String str, int bucketCount) {
        int hash = hash3(str, bucketCount);
        return hash >>> 16;
    }

    int hash3(String str, int bucketCount) {
        int hash = 0;
        int x = 0;
        char[] chars = str.toCharArray();
        for (char aChar : chars) {
            hash = (hash << 4) + aChar;
            if ((x = hash & 0xf0000000) != 0) {
                hash ^= (x >> 24);
                hash &= ~x;
            }
        }
        hash = (hash & 0x7fffffff);
        return hash;
    }

    int hash4(String str, int bucketCount) {
        int code = hash5(str, bucketCount);
        return code >>> 16;
    }


    int hash5(String str, int bucketCount) {
        int code = Integer.parseInt(str.substring(0, 2), 16);
        return code;
    }

    @Override
    public Integer call(String str, Integer bucketCount, Integer num) throws Exception {

        int code = 0;
        if (num == 5) {
            code = hash5(str, bucketCount);
        } else if (num == 4) {
            code = hash4(str, bucketCount);
        } else if (num == 3) {
            code = hash3(str, bucketCount);
        } else if (num == 2) {
            code = hash2(str, bucketCount);
        } else if (num == 1) {
            code = hash1(str, bucketCount);
        } else if (num == 0) {
            code = hash0(str, bucketCount);
        }

        return code % bucketCount;
    }

    @Override
    public DataType getDataType() {
        return DataTypes.IntegerType;
    }

    @Override
    protected String use_desc() {
        return "sharding_hash(string, hashCount), 返回值: hash值取模; 第一个参数:需要进行Hash的字符串, 第二个参数:hash取模的个数";
    }

    @Override
    public String getRegisterName() {
        return "sharding_hash";
    }
}
