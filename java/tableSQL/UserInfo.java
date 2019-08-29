package tableSQL;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * 用户类
 *
 * @author lixiyan
 * @data 2019/8/5 3:48 PM
 */
@Data
@ToString
public class UserInfo implements Serializable {
    private Timestamp pTime;
    private String userId;
    private String itemId;

    public UserInfo() {
    }

    public UserInfo(String userId, String itemId) {
        this.userId = userId;
        this.itemId = itemId;
        this.pTime = new Timestamp(System.currentTimeMillis());
    }
}
