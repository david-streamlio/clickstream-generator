package io.streamnative.data.feeds.generated.clickstream.data;

import org.apache.pulsar.io.core.PushSource;

import java.io.*;
import java.time.Instant;
import java.util.zip.GZIPInputStream;

public class FileBasedClickGenerator implements ClickGenerator, Closeable {

    private InputStream inputStream;

    // Create a BufferedReader to read the lines of the file
    private BufferedReader reader;

    private PushSource pushSource;

    public FileBasedClickGenerator(PushSource pushSource, String resourceName) {
        this.pushSource = pushSource;

        try {
           // inputStream = new FileInputStream(resourceName);
            inputStream = getClass().getClassLoader().getResourceAsStream(resourceName);
            reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(inputStream)));
        } catch (final IOException ioEx) {
            System.err.println(ioEx);
        }
    }

    @Override
    public void generate() {
        int counter = 0;
        Click click;
        while ((click = getClick()) != null) {
            this.pushSource.consume(new ClickRecord(click));
            counter++;
        }
        System.out.println(String.format("Generated %d events ", counter));
    }

    public void close() throws IOException {
        this.reader.close();
        this.inputStream.close();
    }

    private Click getClick() {
        if (reader == null) {
            return null;
        }

        String line;
        try {
            if ((line = reader.readLine()) != null) {
                return parse(line); // Return the next line if available
            }
        } catch (IOException e) {
            System.out.println("An error occurred while reading the file: " + e.getMessage());
        }
        return null;
    }

    private Click parse(String s) {
        Click click = new Click();
        String[] fields = s.split(",");

        click.setUser_id(fields[0]);
        click.setItem_id(fields[1]);
        click.setCategory_id(fields[2]);
        click.setBehavior(fields[3]);
        click.setTs(Instant.ofEpochMilli(Long.parseLong(fields[4]) * 1000).toString());
        return click;
    }
}
