import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class api_batch_sample {
    private static final String TABLE_NAME = "TEST1";
    private static final String COLUMN_FAMILY_DF = "df";
    private static final String COLUMN_FAMILY_EX = "ex";

    public static void main(String[] args) throws Exception {
        System.out.print("[------]batch put start.\n");
        Configuration conf = HBaseConfiguration.create();
        System.out.print("[------]create conf succeed.\n");
        conf.set("hbase.zookeeper.quorum", "192.168.6.3,192.168.6.4,192.168.6.5");
        HConnection connection = HConnectionManager.createConnection(conf);
        System.out.print("[------]create connection succeed.\n");
        HTableInterface hTableInterface = connection.getTable(TABLE_NAME);
        System.out.print("[------]get table succeed.\n");

        Random random = new Random();
        String[] rows = new String[] {"01", "02", "03"};
        String[] names = new String[] {"zhang san", "li si", "wang wu", "wei liu"};
        String[] sexs = new String[] {"men", "women"};
        String[] heights = new String[] {"165cm", "170cm", "175cm", "180cm"};
        String[] weights = new String[] {"50kg", "55kg", "60kg", "65kg", "70kg", "75kg", "80kg"};

        List<Row> bath = new ArrayList<>();
        for(String row : rows) {
            Put put = new Put(Bytes.toBytes("row" + row));

            String name = names[random.nextInt(names.length)];
            put.addColumn(COLUMN_FAMILY_DF.getBytes(), "name".getBytes(), name.getBytes());

            String sex = sexs[random.nextInt(sexs.length)];
            put.addColumn(COLUMN_FAMILY_DF.getBytes(), "sex".getBytes(), sex.getBytes());

            String height = heights[random.nextInt(heights.length)];
            put.addColumn(COLUMN_FAMILY_EX.getBytes(), "height".getBytes(), height.getBytes());

            String weight = weights[random.nextInt(weights.length)];
            put.addColumn(COLUMN_FAMILY_EX.getBytes(), "weight".getBytes(), weight.getBytes());
            bath.add(put);
        }

        Object[] results = new Object[bath.size()];
        try {
            hTableInterface.batch(bath, results);
        } catch (InterruptedException e) {
            System.out.print("[------]batch put failed.\n");
            e.printStackTrace();
            return;
        }
        System.out.print("[------]batch put succeed.\n");
        bath.clear();
        hTableInterface.close();
        System.out.print("[------]table closed.\n");
        connection.close();
        System.out.print("[------]connection closed.\n");
        System.out.print("[------]batch put end.\n");
    }
}
