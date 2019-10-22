package cep;

/**
 * 电压事件
 *
 * @author lixiyan
 * @data 2019/8/29 4:25 PM
 */
public class PowerEvent {
    private String voltage;

    public PowerEvent(String voltage) {
        this.voltage = voltage;
    }

    public String getVoltage() {
        return voltage;
    }

    public void setVoltage(String voltage) {
        this.voltage = voltage;
    }

    @Override
    public String toString() {
        return "PowerEvent{" +
                "voltage='" + voltage + '\'' +
                '}';
    }
}
