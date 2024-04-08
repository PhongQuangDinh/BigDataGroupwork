import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.OVertex;
import java.lang.Thread;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.math.BigDecimal;


public class Transaction {
    public static void main(String[] args) {
        Thread t1 = new Thread(() -> {
            try (OrientDB orientDB = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
                 ODatabaseDocument db = orientDB.open("demodb", "admin", "admin")) {
                db.begin();
/*                try (OResultSet rs = db.query("SELECT * FROM Customer where @RID = '#234:4'")) {
                    while (rs.hasNext()) {
                        OResult row = rs.next();
                        System.out.println("name: " + row.getProperty("name"));
                        System.out.println("email: " + row.getProperty("email"));
                    }
                }*/
/*                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                try (OResultSet rs = db.query("SELECT * FROM Customer")) {
                    while (rs.hasNext()) {
                        OResult row = rs.next();
                        System.out.println("name: " + row.getProperty("name"));
                        System.out.println("email: " + row.getProperty("email"));
                    }
                }*/

                ODocument order = db.load(new ORecordId(242, 0));

                if (order != null) {
                    // Retrieve the current total value
                    BigDecimal currentTotal = order.field("total");

                    // Update the total value by adding 10
                    BigDecimal updatedTotal = currentTotal.add(BigDecimal.TEN);

                    // Set the updated total value
                    order.field("total", updatedTotal);
                }
                db.save(order);

                db.commit();
                db.close();
            }
        });

        Thread t2 = new Thread(() -> {
            try (OrientDB orientDB = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
                 ODatabaseDocument db = orientDB.open("demodb", "tmp", "tmp")) {
                db.begin();

                /*                OResultSet rs = db.command("update #234:4 set name = 'test2';");*/
                /*ODocument record = db.load(new ORecordId(234, 1));

                if (record != null) {
                    // Update the "name" property to "tmp2"
                    record.field("name", "tmp6");

                    db.save(record);
                } else {
                    // Handle the case where the record is not found
                    System.out.println("Record with ID #234:1 not found.");
                }*/

                ODocument order = db.load(new ORecordId(242, 0));

                if (order != null) {
                    // Retrieve the current total value
                    BigDecimal currentTotal = order.field("total");

                    // Update the total value by adding 10
                    BigDecimal updatedTotal = currentTotal.add(BigDecimal.TEN);

                    // Set the updated total value
                    order.field("total", updatedTotal);
                }
                db.save(order);

                db.commit();
                db.close();
            }
        });
        t2.start();
        t1.start();
    }
}