import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class FileAccess
{
    private FileSystem hdfs;

    private static String symbols = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
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
            hdfs.create(new Path(path));
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
            OutputStream os = hdfs.create(new Path(path));
            BufferedWriter br = new BufferedWriter(
                new OutputStreamWriter(os, "UTF-8")
        );

        br.write(content);
        br.flush();
        br.close();
        hdfs.close();
    }catch (Exception e){
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
        return new Path(path).toString();
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
       return  null;
    }

    private static String getRandomWord()
    {
        StringBuilder builder = new StringBuilder();
        int length = 2 + (int) Math.round(10 * Math.random());
        int symbolsCount = symbols.length();
        for(int i = 0; i < length; i++) {
            builder.append(symbols.charAt((int) (symbolsCount * Math.random())));
        }
        return builder.toString();
    }
}
