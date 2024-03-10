package io.streamnative.data.feeds.generated.clickstream.data;


import lombok.Data;
import java.util.Date;

@Data
public class Click {

    private String user_id;

    private String item_id;

    private String category;

    private String behavior;

    private String ts;
}
