import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class FileAccess
{
    private FileSystem hdfs;
    /**
     * Initializes the class, using rootPath as "/" directory
     *
     * @param rootPath - the path to the root of HDFS,
     * for example, hdfs://localhost:32771
     */
    public FileAccess(String rootPath)
    {
        try {
            Configuration configuration = new Configuration();
            configuration.set("dfs.client.use.datanode.hostname", "true");
            System.setProperty("HADOOP_USER_NAME", "root");
            hdfs = FileSystem.get(new URI(rootPath), configuration);

        }catch (Exception e){
            e.printStackTrace();
        }

    }

    /**
     * Creates empty file or directory
     *
     * @param path
     */
    public void create(String path)
    {
        try {
            if (hdfs.exists(new Path(path))){
                delete(path);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            hdfs.createNewFile(new Path(path));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Appends content to the file
     *
     * @param path
     * @param content
     */
    public void append(String path, String content)
    {
        try {
            OutputStream os = hdfs.append(new Path(path));
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
            br.write(content);
            br.flush();
            br.close();
    }catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * Returns content of the file
     *
     * @param path
     * @return
     */
    public String read(String path) {
        StringBuilder builder = new StringBuilder();
        Path file = new Path(path);
        try {
            BufferedReader breader = new BufferedReader(new InputStreamReader(hdfs.open(file)));
            String str;
            while ((str = breader.readLine()) != null) {
                builder.append(str);
                builder.append("\n");
            }
            return builder.toString();
        }catch (Exception e){
            e.printStackTrace();
        }
        return builder.toString();
    }

    /**
     * Deletes file or directory
     *
     * @param path
     */
    public void delete(String path)
    {
        try {
            hdfs.delete(new Path(path), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Checks, is the "path" is directory or file
     *
     * @param path
     * @return
     */
    public boolean isDirectory(String path)
    {
        try {
            if (hdfs.exists(new Path(path))) {
                return true;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Return the list of files and subdirectories on any directory
     *
     * @param path
     * @return
     */
    public List<String> list(String path)
    {
        ArrayList<String> files = new ArrayList<>();
        try {
            files.add(hdfs.listFiles(new Path(path), true).next().toString());
            System.out.println(hdfs.listFiles(new Path(path), true).next().getPath().getName());

        } catch (Exception e){
            e.printStackTrace();
        }
        return files;
    }

}
