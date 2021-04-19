package api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

public class table_scan_sample3 {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "udp01,udp02,udp03");
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(constants.TABLE_NAME));

        Scan scan = new Scan();
        scan.addColumn(constants.COLUMN_FAMILY_DF.getBytes(), "name".getBytes());
        scan.addFamily(constants.COLUMN_FAMILY_EX.getBytes());

        ResultScanner rs = table.getScanner(scan);
        for (Result r = rs.next(); r != null; r = rs.next()) {
            byte[] row_key = r.getRow();
            System.out.print("[------]row_key=" + new String(row_key) + "\n");
            byte[] name = r.getValue(constants.COLUMN_FAMILY_DF.getBytes(), "name".getBytes());
            System.out.print("[------]name=" + new String(name) + "\n");
            byte[] weight = r.getValue(constants.COLUMN_FAMILY_EX.getBytes(), "weight".getBytes());
            System.out.print("[------]weight=" + new String(weight) + "\n");
        }

        table.close();
        connection.close();
    }
}
