import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.*;

public class HDFSJavaAPI {
    static FileSystem fs;
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        fs = FileSystem.get(conf);
        
        System.out.println("=== 1. Upload file (overwrite mode) ===");
        upload("hello.txt", "/user/yuki/hello_java.txt", false);
        
        System.out.println("=== 2. Download file (auto rename) ===");
        download("/user/yuki/hello_java.txt", "hello_java.txt");
        
        System.out.println("=== 3. Output file content ===");
        cat("/user/yuki/hello_java.txt");
        
        System.out.println("=== 4. Show file info ===");
        info("/user/yuki/hello_java.txt");
        
        System.out.println("=== 5. Recursive directory listing ===");
        listDir("/user/yuki");
        
        System.out.println("=== 6. Create directory ===");
        mkdir("/user/yuki/testdir");
        
        System.out.println("=== 7. Append content ===");
        append("/user/yuki/hello_java.txt", "\nAppended by Java API\n");
        
        System.out.println("=== 8. Delete file ===");
        rm("/user/yuki/hello_java.txt");
        
        System.out.println("=== 9. Delete empty directory ===");
        rmdir("/user/yuki/testdir");
        
        System.out.println("=== 10. Move / Rename ===");
        fs.rename(new Path("/user/yuki/hello.txt"), new Path("/user/yuki/hello_moved.txt"));
        System.out.println("Moved /user/yuki/hello.txt -> /user/yuki/hello_moved.txt");
        
        fs.close();
    }
    
    static void upload(String local, String remote, boolean appendMode) throws IOException {
        Path src = new Path(local);
        Path dst = new Path(remote);
        if (fs.exists(dst)) {
            if (appendMode) {
                FSDataOutputStream out = fs.append(dst);
                BufferedReader br = new BufferedReader(new FileReader(local));
                String line;
                while ((line = br.readLine()) != null) out.writeBytes(line + "\n");
                br.close(); out.close();
                System.out.println("Appended to " + remote);
            } else {
                fs.copyFromLocalFile(false, true, src, dst);
                System.out.println("Overwritten " + remote);
            }
        } else {
            fs.copyFromLocalFile(false, false, src, dst);
            System.out.println("Uploaded " + remote);
        }
    }
    
    static void download(String remote, String local) throws IOException {
        Path src = new Path(remote);
        File localFile = new File(local);
        String name = local;
        int i = 1;
        while (localFile.exists()) {
            name = local + "_" + i;
            localFile = new File(name);
            i++;
        }
        fs.copyToLocalFile(false, src, new Path(name), true);
        System.out.println("Downloaded to " + name);
    }
    
    static void cat(String remote) throws IOException {
        FSDataInputStream in = fs.open(new Path(remote));
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line;
        while ((line = br.readLine()) != null) System.out.println(line);
        br.close();
    }
    
    static void info(String remote) throws IOException {
        FileStatus status = fs.getFileStatus(new Path(remote));
        System.out.println("Permission: " + status.getPermission());
        System.out.println("Size: " + status.getLen());
        System.out.println("Time: " + status.getModificationTime());
        System.out.println("Path: " + status.getPath());
    }
    
    static void listDir(String remote) throws IOException {
        FileStatus[] list = fs.listStatus(new Path(remote));
        for (FileStatus f : list) {
            System.out.println(f.getPermission() + " " + f.getLen() + " " + f.getModificationTime() + " " + f.getPath());
            if (f.isDirectory()) listDir(f.getPath().toString());
        }
    }
    
    static void mkdir(String remote) throws IOException {
        fs.mkdirs(new Path(remote));
        System.out.println("Created " + remote);
    }
    
    static void append(String remote, String content) throws IOException {
        FSDataOutputStream out = fs.append(new Path(remote));
        out.writeBytes(content);
        out.close();
        System.out.println("Appended to " + remote);
    }
    
    static void rm(String remote) throws IOException {
        fs.delete(new Path(remote), false);
        System.out.println("Deleted " + remote);
    }
    
    static void rmdir(String remote) throws IOException {
        Path p = new Path(remote);
        if (fs.exists(p)) {
            FileStatus[] list = fs.listStatus(p);
            if (list.length > 0) {
                System.out.println("Directory not empty, not deleted");
            } else {
                fs.delete(p, true);
                System.out.println("Deleted empty dir " + remote);
            }
        }
    }
}
