import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

public class split_region_sample {
    private static final String TABLE_NAME = "TEST1";
    private static final String COLUMN_FAMILY_DF = "df";
    private static final String COLUMN_FAMILY_EX = "ex";

    public static void main(String[] args) throws Exception {
        System.out.print("[------]SplitRegion Start.\n");
        Configuration configuration= HBaseConfiguration.create();
        System.out.print("[------]step 1 succeed.\n");
        configuration.set("hbase.zookeeper.quorum", "192.168.6.3,192.168.6.4,192.168.6.5");
        HBaseAdmin admin = new HBaseAdmin(configuration);
        System.out.print("[------]step 2 succeed.\n");

        String table_name = TABLE_NAME;
        if (admin.tableExists(table_name)) {
            admin.disableTable(table_name);
            System.out.println("[----]disableTable table[" + table_name + "]\n");
            admin.deleteTable(table_name);
            System.out.println("[----]deleteTable table[" + table_name + "]\n");
        }

        HTableDescriptor desc = new HTableDescriptor(table_name);
        HColumnDescriptor family1 = new HColumnDescriptor(COLUMN_FAMILY_DF.getBytes());
        family1.setTimeToLive(3 * 60 * 60 * 24);     //过期时间
        family1.setBloomFilterType(BloomType.ROW);   //按行过滤
        family1.setMaxVersions(3);                   //版本数
        desc.addFamily(family1);
        HColumnDescriptor family2 = new HColumnDescriptor(COLUMN_FAMILY_EX.getBytes());
        family2.setTimeToLive(2 * 60 * 60 * 24);     //过期时间
        family2.setBloomFilterType(BloomType.ROW);   //按行过滤
        family2.setMaxVersions(2);                   //版本数
        desc.addFamily(family2);
        System.out.print("[------]step 3 succeed.\n");

        byte[][] splitKeys = {
                Bytes.toBytes("0"),
                Bytes.toBytes("2"),
                Bytes.toBytes("4"),
                Bytes.toBytes("6"),
                Bytes.toBytes("8"),
        };

        admin.createTable(desc, splitKeys);
        System.out.println("[----]createTable table[" + table_name + "]\n");
        System.out.print("[------]SplitRegion end.\n");
    }
}
