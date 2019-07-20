package entity;

import lombok.Data;

/**
 * 学生实体类
 *
 * @data: 2019/7/20 10:59 AM
 * @author:lixiyan
 */

@Data
public class Student {
    private int id;

    private String name;

    private String password;

    private int age;

}
