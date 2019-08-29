package broadcaststate;

import java.io.Serializable;
import com.alibaba.fastjson.JSONObject;
import java.util.Map;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;


/**
 * 计算结果
 *
 * @author lixiyan
 * @data 2019/8/21 4:57 PM
 */
public class EvaluatedResult implements Serializable {
    private String userId;
    private String channel;
    private Integer purchasePathLength;
    private Map<String, Integer> eventTypeCounts;

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserId() {
        return userId;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getChannel() {
        return channel;
    }

    public void setPurchasePathLength(Integer purchasePathLength) {
        this.purchasePathLength = purchasePathLength;
    }

    public Integer getPurchasePathLength() {
        return purchasePathLength;
    }

    public void setEventTypeCounts(
            Map<String, Integer> eventTypeCounts) {
        this.eventTypeCounts = eventTypeCounts;
    }

    public Map<String, Integer> getEventTypeCounts() {
        return eventTypeCounts;
    }

    public static EvaluatedResult fromString(String result) {
        JSONObject o = JSONObject.parseObject(result);
        EvaluatedResult r = new EvaluatedResult();
        r.userId = o.getString("userId");
        r.channel = o.getString("channel");

        String dataStr = o.getString("data");
        JSONObject d = JSONObject.parseObject(dataStr);
        Map<String, Integer> counts = Maps.newHashMap();
        d.keySet().forEach(key -> counts.put(key, counts.get(key)));
        r.eventTypeCounts = counts;
        return r;
    }

    public String toJSONString() {
        JSONObject o = new JSONObject(true);
        o.put("userId", userId);
        o.put("channel", channel);
        o.put("purchasePathLength", purchasePathLength);
        JSONObject stat = new JSONObject();
        stat.putAll(eventTypeCounts);
        o.put("eventTypeCounts", stat);
        return o.toJSONString();
    }
}
