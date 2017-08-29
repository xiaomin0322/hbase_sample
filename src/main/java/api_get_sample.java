import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Random;

public class api_get_sample {
    private static final String TABLE_NAME = "TEST1";
    private static final String COLUMN_FAMILY_DF = "df";
    private static final String COLUMN_FAMILY_EX = "ex";

    public static void main(String[] args) throws Exception {
        System.out.print("[------]get sample start.\n");
        Configuration conf = HBaseConfiguration.create();
        System.out.print("[------]create conf succeed.\n");
        conf.set("hbase.zookeeper.quorum", "192.168.6.3,192.168.6.4,192.168.6.5");
        HConnection connection = HConnectionManager.createConnection(conf);
        System.out.print("[------]create connection succeed.\n");
        HTableInterface hTableInterface = connection.getTable(TABLE_NAME);

        Random random = new Random();
        String[] rows = new String[] {"01", "02", "03"};
        //默认取得所有列
        Get get = new Get(("row" + rows[random.nextInt(rows.length)]).getBytes());
//        //可以使用addColumn()方法来只取得某列
//        get.addColumn(COLUMN_FAMILY_DF.getBytes(), "name".getBytes());
//        get.addColumn(COLUMN_FAMILY_DF.getBytes(), "sex".getBytes());
//        get.addColumn(COLUMN_FAMILY_EX.getBytes(), "height".getBytes());
//        get.addColumn(COLUMN_FAMILY_EX.getBytes(), "weight".getBytes());
//        //可以使用addFamily()方法来之取得某个列族
//        get.addFamily(COLUMN_FAMILY_DF.getBytes());

        Result result = hTableInterface.get(get);
        System.out.print("[------]result=" + result + "\n");
        JSONObject jsonObject = convertToJson(result);
        System.out.print("[------]jsonObject=" + jsonObject + "\n");
        System.out.print("[------]get sample end.\n");
    }

    private static JSONObject convertToJson(Result result) throws IOException {
        JSONObject jsonObject = new JSONObject();
        putToJson(jsonObject, result, COLUMN_FAMILY_DF, "name");
        putToJson(jsonObject, result, COLUMN_FAMILY_DF, "sex");
        putToJson(jsonObject, result, COLUMN_FAMILY_EX, "height");
        putToJson(jsonObject, result, COLUMN_FAMILY_EX, "weight");
        return jsonObject;
    }

    private static void putToJson(
            JSONObject jsonObject, Result result, String family, String column) {
        byte[] value = result.getValue(family.getBytes(), column.getBytes());
        if (value != null) {
            jsonObject.put(column, new String(value));
        }
    }
}
