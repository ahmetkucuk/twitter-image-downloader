import java.io.*;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by ahmetkucuk on 29/12/15.
 */
public class ImageDownloader {

    private String directory;
    private ExecutorService executorService;

    private final Set<String> urls = new HashSet<>();

    private int counter = 0;

    private static ImageDownloader instance;

    private ImageDownloader(String dir) {
        this.directory = dir;
    }

    public static void init(String dir, int numberOfThread) {
        instance = new ImageDownloader(dir);
        instance.setExecutorService(Executors.newFixedThreadPool(numberOfThread));
    }

    private void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public static ImageDownloader getInstance() {return instance;}

    public int addToDownloadQueue(final String url) {

        //Check if url is already downloaded
        if(urls.contains(url)) return counter;
        urls.add(url);
        counter++;
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                downloadImage(url, directory, counter + ".jpg");
            }
        });
        return counter;
    }

    public void waitUntilDone() {

        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void downloadImage(String sourceUrl, String targetDirectory, String targetFileName) {
        try {
            URL imageUrl = new URL(sourceUrl);
            try (InputStream imageReader = new BufferedInputStream(
                    imageUrl.openStream());
                 OutputStream imageWriter = new BufferedOutputStream(
                         new FileOutputStream(targetDirectory + File.separator
                                 + targetFileName));)
            {
                int readByte;

                while ((readByte = imageReader.read()) != -1)
                {
                    imageWriter.write(readByte);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
