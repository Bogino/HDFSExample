import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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
    public void create(String path) throws IOException
    {
        if (hdfs.exists(new Path(path)))
                delete(path);

        hdfs.createNewFile(new Path(path));

    }

    /**
     * Appends content to the file
     *
     * @param path
     * @param content
     */
    public void append(String path, String content) {
        try (BufferedWriter br = new BufferedWriter(
                new OutputStreamWriter(hdfs.append(new Path(path)), StandardCharsets.UTF_8))) {
            br.write(content);
            br.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns content of the file
     *
     * @param path
     * @return
     */
    public String read(String path) throws IOException {
        Path file = new Path(path);
        BufferedReader breader = new BufferedReader(new InputStreamReader(hdfs.open(file)));
        StringBuilder builder = new StringBuilder();
        String str;
        while ((str = breader.readLine()) != null) {
            builder.append(str);
            builder.append("\n");
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
    public boolean isDirectory(String path) throws IOException {
        return hdfs.getFileStatus(new Path(path)).isDirectory();
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
