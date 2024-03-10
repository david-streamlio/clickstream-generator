package io.streamnative.data.feeds.generated.clickstream.data;

import org.apache.pulsar.io.core.PushSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class FileBasedClickGenerator implements ClickGenerator {

    private static Instant now = Instant.now().minus(20, ChronoUnit.DAYS).truncatedTo(ChronoUnit.DAYS);

    private InputStream inputStream;

    // Create a BufferedReader to read the lines of the file
    private BufferedReader reader;

    private PushSource pushSource;

    public FileBasedClickGenerator(PushSource pushSource) {
        this.pushSource = pushSource;
        inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("user_behavior.txt");
        reader = new BufferedReader(new InputStreamReader(inputStream));
    }

    @Override
    public void generate() {
        Click click;
        while ((click = getClick()) != null) {
            this.pushSource.consume(new ClickRecord(click));
        }
        this.pushSource.consume(new ClickRecord(null));
    }

    public Click getClick() {
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

    private static Click parse(String s) {
        Click click = new Click();
        String[] fields = s.split(",");

        click.setUser_id(fields[0]);
        click.setItem_id(fields[1]);
        click.setCategory(fields[2]);
        click.setBehavior(fields[3]);
        click.setTs(now.plus(Long.parseLong(fields[4]), ChronoUnit.MILLIS)
                .truncatedTo(ChronoUnit.SECONDS).toString());
        return click;
    }
}
