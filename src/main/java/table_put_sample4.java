import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class table_put_sample4 {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.1.80,192.168.1.81,192.168.1.82");
        conf.set("hbase.client.write.buffer", "1048576");//1M
        Connection connection = ConnectionFactory.createConnection(conf);
        BufferedMutator table = connection.getBufferedMutator(TableName.valueOf(constants.TABLE_NAME));
        System.out.print("[--------]write buffer size = " + table.getWriteBufferSize());

        Random random = new Random();
        String[] rows = new String[] {"01", "02", "03", "04", "05"};
        String[] names = new String[] {"zhang san", "li si", "wang wu", "wei liu"};
        String[] sexs = new String[] {"men", "women"};
        String[] heights = new String[] {"165cm", "170cm", "175cm", "180cm"};
        String[] weights = new String[] {"50kg", "55kg", "60kg", "65kg", "70kg", "75kg", "80kg"};

        List<Mutation> batch = new ArrayList<>();
        for(String row : rows) {
            Put put = new Put(Bytes.toBytes("row" + row));
            String name = names[random.nextInt(names.length)];
            put.addColumn(constants.COLUMN_FAMILY_DF.getBytes(), "name".getBytes(), name.getBytes());
            String sex = sexs[random.nextInt(sexs.length)];
            put.addColumn(constants.COLUMN_FAMILY_DF.getBytes(), "sex".getBytes(), sex.getBytes());
            String height = heights[random.nextInt(heights.length)];
            put.addColumn(constants.COLUMN_FAMILY_EX.getBytes(), "height".getBytes(), height.getBytes());
            String weight = weights[random.nextInt(weights.length)];
            put.addColumn(constants.COLUMN_FAMILY_EX.getBytes(), "weight".getBytes(), weight.getBytes());
            batch.add(put);
        }

        table.mutate(batch);
        table.flush();
        table.close();
        connection.close();
    }
}
