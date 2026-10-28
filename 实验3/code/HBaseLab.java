import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/** All mutations are confined to the explicit lab3_java_ table prefix. */
public class HBaseLab implements AutoCloseable {
    private final Connection connection;
    private static final String PREFIX = "lab3_java_";
    private int checks;
    public HBaseLab() throws IOException {
        Configuration config = HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(config);
    }
    private TableName name(String table) {
        if (!table.startsWith(PREFIX)) throw new IllegalArgumentException("Only " + PREFIX + " tables are allowed");
        return TableName.valueOf(table);
    }
    private static String[] field(String s) {
        String[] p = s.split(":", 2);
        return p.length == 1 ? new String[]{p[0], "value"} : p;
    }
    public void createTable(String tableName, String[] fields) throws IOException {
        TableName table = name(tableName);
        try (Admin admin = connection.getAdmin()) {
            if (admin.tableExists(table)) {
                if (admin.isTableEnabled(table)) admin.disableTable(table);
                admin.deleteTable(table);
            }
            TableDescriptorBuilder b = TableDescriptorBuilder.newBuilder(table);
            TreeSet<String> families = new TreeSet<>();
            for (String f : fields) families.add(field(f)[0]);
            if (families.isEmpty()) throw new IllegalArgumentException("At least one family required");
            for (String f : families) b.setColumnFamily(ColumnFamilyDescriptorBuilder.of(f));
            admin.createTable(b.build());
        }
    }
    public void addRecord(String tableName, String row, String[] fields, String[] values) throws IOException {
        if (fields.length != values.length || fields.length == 0) throw new IllegalArgumentException("fields/values mismatch");
        Put put = new Put(Bytes.toBytes(row));
        for (int i = 0; i < fields.length; i++) {
            String[] f = field(fields[i]);
            put.addColumn(Bytes.toBytes(f[0]), Bytes.toBytes(f[1]), Bytes.toBytes(values[i]));
        }
        try (Table t = connection.getTable(name(tableName))) { t.put(put); }
    }
    public SortedMap<String,SortedMap<String,String>> scanColumn(String tableName, String column) throws IOException {
        SortedMap<String,SortedMap<String,String>> output = new TreeMap<>();
        // Scan all rows first: a server-side column filter would omit missing rows entirely.
        try (Table t = connection.getTable(name(tableName)); ResultScanner scanner = t.getScanner(new Scan())) {
            for (Result r : scanner) {
                SortedMap<String,String> projected = new TreeMap<>();
                for (Cell cell : r.rawCells()) {
                    String fam = Bytes.toString(CellUtil.cloneFamily(cell));
                    String col = fam + ":" + Bytes.toString(CellUtil.cloneQualifier(cell));
                    if (column.equals(fam) || column.equals(col)) projected.put(col, Bytes.toString(CellUtil.cloneValue(cell)));
                }
                output.put(Bytes.toString(r.getRow()), projected.isEmpty() ? null : projected);
            }
        }
        for (Map.Entry<String,SortedMap<String,String>> e : output.entrySet())
            System.out.println("COLUMN " + column + " " + e.getKey() + " " + e.getValue());
        return output;
    }
    // The handout omits the new-value argument. Keep the exact signature using terminal input.
    public void modifyData(String tableName, String row, String column) throws IOException {
        System.out.print("New value: ");
        String value = new BufferedReader(new InputStreamReader(System.in)).readLine();
        if (value == null) throw new EOFException("New value required");
        modifyData(tableName, row, column, value);
    }
    public void modifyData(String tableName, String row, String column, String value) throws IOException {
        addRecord(tableName, row, new String[]{column}, new String[]{value});
    }
    public void deleteRow(String tableName, String row) throws IOException {
        try (Table t = connection.getTable(name(tableName))) { t.delete(new Delete(Bytes.toBytes(row))); }
    }
    public void addFamily(String table, String family) throws IOException {
        try (Admin a = connection.getAdmin()) { a.addColumnFamily(name(table), ColumnFamilyDescriptorBuilder.of(family)); }
    }
    public void deleteFamily(String table, String family) throws IOException {
        try (Admin a = connection.getAdmin()) { a.deleteColumnFamily(name(table), Bytes.toBytes(family)); }
    }
    public void deleteColumn(String table, String row, String column) throws IOException {
        String[] p = field(column);
        try (Table t = connection.getTable(name(table))) {
            t.delete(new Delete(Bytes.toBytes(row)).addColumns(Bytes.toBytes(p[0]), Bytes.toBytes(p[1])));
        }
    }
    public void clearTable(String table) throws IOException {
        try (Admin a = connection.getAdmin()) { a.disableTable(name(table)); a.truncateTable(name(table), true); }
    }
    public long countRows(String table) throws IOException {
        long count = 0;
        try (Table t = connection.getTable(name(table)); ResultScanner s = t.getScanner(new Scan().setCaching(100))) {
            for (Result ignored : s) count++;
        }
        System.out.println("COUNT " + table + " " + count);
        return count;
    }
    public void listTables() throws IOException {
        try (Admin a = connection.getAdmin()) {
            for (TableDescriptor d : a.listTableDescriptors())
                System.out.println("TABLE " + d.getTableName() + " " + Arrays.toString(d.getColumnFamilyNames().stream().map(Bytes::toString).sorted().toArray()));
        }
    }
    public void showRecords(String table) throws IOException {
        try (Table t = connection.getTable(name(table)); ResultScanner s = t.getScanner(new Scan())) {
            for (Result r : s) for (Cell cell : r.rawCells())
                System.out.println("DATA\t" + table.substring(PREFIX.length()) + "\t" + Bytes.toString(r.getRow()) + "\t"
                    + Bytes.toString(CellUtil.cloneFamily(cell)) + ":" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }
    private String get(String table, String row, String col) throws IOException {
        String[] f = field(col);
        try (Table t = connection.getTable(name(table))) {
            byte[] v = t.get(new Get(Bytes.toBytes(row))).getValue(Bytes.toBytes(f[0]), Bytes.toBytes(f[1]));
            return v == null ? null : Bytes.toString(v);
        }
    }
    private void check(String label, boolean ok) {
        if (!ok) throw new AssertionError(label);
        checks++;
        System.out.println("CHECK " + label + " PASS");
    }
    public void demo() throws IOException {
        String student = PREFIX + "Student", course = PREFIX + "Course", sc = PREFIX + "SC", scratch = PREFIX + "Scratch";
        createTable(student, new String[]{"info"});
        String[] sf = {"info:S_No", "info:S_Name", "info:S_Sex", "info:S_Age"};
        addRecord(student, "Zhangsan", sf, new String[]{"2015001", "Zhangsan", "male", "23"});
        addRecord(student, "Mary", sf, new String[]{"2015003", "Mary", "female", "22"});
        addRecord(student, "Lisi", sf, new String[]{"2015003", "Lisi", "male", "24"});
        createTable(course, new String[]{"info"});
        String[] cf = {"info:C_No", "info:C_Name", "info:C_Credit"};
        addRecord(course, "123001", cf, new String[]{"123001", "Math", "2.0"});
        addRecord(course, "123002", cf, new String[]{"123002", "Computer Science", "5.0"});
        addRecord(course, "123003", cf, new String[]{"123003", "English", "3.0"});
        createTable(sc, new String[]{"info"});
        String[][] rows = {{"2015001","123001","86"},{"2015001","123003","69"},{"2015002","123002","77"},
                           {"2015002","123003","99"},{"2015003","123001","98"},{"2015003","123002","95"}};
        for (String[] r : rows) addRecord(sc, r[0] + "_" + r[1], new String[]{"info:SC_Sno","info:SC_Cno","info:SC_Score"}, r);
        check("student_count_3", countRows(student) == 3);
        check("course_count_3", countRows(course) == 3);
        check("sc_count_6", countRows(sc) == 6);
        check("duplicate_id_preserved", get(student,"Mary","info:S_No").equals(get(student,"Lisi","info:S_No")));
        check("orphan_sc_preserved", "77".equals(get(sc,"2015002_123002","info:SC_Score")));
        listTables();
        showRecords(student); showRecords(course); showRecords(sc);
        createTable(scratch, new String[]{"info", "Score"});
        addRecord(scratch,"Zhangsan",new String[]{"info:name","Score:Math","Score:Computer Science","Score:English"},new String[]{"Zhangsan","86","77","69"});
        addRecord(scratch,"Mary",new String[]{"info:name"},new String[]{"Mary"});
        check("scratch_count_2", countRows(scratch) == 2);
        SortedMap<String,SortedMap<String,String>> family = scanColumn(scratch,"Score");
        check("family_three_qualifiers", family.get("Zhangsan").size() == 3);
        check("missing_family_null", family.containsKey("Mary") && family.get("Mary") == null);
        SortedMap<String,SortedMap<String,String>> single = scanColumn(scratch,"Score:Math");
        check("single_qualifier_only", single.get("Zhangsan").size() == 1);
        check("missing_column_null", single.containsKey("Mary") && single.get("Mary") == null);
        modifyData(scratch,"Zhangsan","Score:Math","90");
        check("modify_90", "90".equals(get(scratch,"Zhangsan","Score:Math")));
        deleteColumn(scratch,"Zhangsan","Score:English");
        check("delete_column", get(scratch,"Zhangsan","Score:English") == null);
        addFamily(scratch,"extra");
        addRecord(scratch,"Mary",new String[]{"extra:note"},new String[]{"temporary"});
        check("add_family", "temporary".equals(get(scratch,"Mary","extra:note")));
        deleteFamily(scratch,"extra");
        try (Admin a = connection.getAdmin()) { check("delete_family", !a.getDescriptor(name(scratch)).hasColumnFamily(Bytes.toBytes("extra"))); }
        deleteRow(scratch,"Mary");
        check("delete_row", countRows(scratch) == 1);
        clearTable(scratch);
        check("truncate_zero", countRows(scratch) == 0);
        addRecord(scratch,"old",new String[]{"info:name"},new String[]{"old"});
        createTable(scratch,new String[]{"info","Score"});
        check("recreate_zero", countRows(scratch) == 0);
        try (Admin a = connection.getAdmin()) { check("truncate_keeps_schema", a.getDescriptor(name(scratch)).hasColumnFamily(Bytes.toBytes("Score"))); }
        boolean rejected = false;
        try { addRecord(scratch,"bad",new String[]{"info:x"},new String[]{}); } catch (IllegalArgumentException e) { rejected = true; }
        check("mismatched_arrays_rejected", rejected);
        // Exercise the exact three-argument signature as well as its four-argument overload.
        addRecord(scratch,"interactive",new String[]{"info:name"},new String[]{"before"});
        modifyData(scratch,"interactive","info:name");
        check("three_argument_modify", "after".equals(get(scratch,"interactive","info:name")));
        clearTable(scratch);
        System.out.println("HBASE_JAVA_CHECKS " + checks + " PASS");
    }
    public void close() throws IOException { connection.close(); }
    public static void main(String[] args) throws Exception { try (HBaseLab lab = new HBaseLab()) { lab.demo(); } }
}
