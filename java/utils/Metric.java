package utils;

import lombok.Data;

import java.util.Map;

/**
 * TODO
 *
 * @data: 2019/7/19 9:17 PM
 * @author:lixiyan
 */
@Data
public class Metric {
    private String name;

    private long timestampe;

    private Map<String, Object> fields;

    private Map<String,String> tags;

}
