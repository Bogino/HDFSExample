public class Main
{
    private static String symbols = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static String path = "/root/file_bogino1.txt";

    public static void main(String[] args) throws Exception {

        FileAccess access = new FileAccess("hdfs://45.135.134.23:8020");
        access.create(path);
        System.out.println(access.isDirectory(path));
        access.append(path, getRandomWord());
        access.list(path);
        System.out.println(access.read(path));

    }

    private static String getRandomWord() {
        StringBuilder builder = new StringBuilder();
        int length = 2 + (int)Math.round(10.0D * Math.random());
        int symbolsCount = symbols.length();

        for(int i = 0; i < length; ++i) {
            builder.append(symbols.charAt((int)((double)symbolsCount * Math.random())));
        }
        builder.append("\\n");

        return builder.toString();
    }
}
