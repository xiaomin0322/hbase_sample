package api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

public class create_table_sample2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "udp01,udp02,udp03");
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        TableName table_name = TableName.valueOf("TEST1");
        if (admin.tableExists(table_name)) {
            admin.disableTable(table_name);
            admin.deleteTable(table_name);
        }

        HTableDescriptor desc = new HTableDescriptor(table_name);
        HColumnDescriptor family1 = new HColumnDescriptor(constants.COLUMN_FAMILY_DF.getBytes());
        family1.setTimeToLive(3 * 60 * 60 * 24);     //过期时间
        family1.setMaxVersions(3);                   //版本数
        desc.addFamily(family1);

        byte[][] splitKeys = {
            Bytes.toBytes("row01"),
            Bytes.toBytes("row02"),
        };

        admin.createTable(desc, splitKeys);
        admin.close();
        connection.close();
    }
}
