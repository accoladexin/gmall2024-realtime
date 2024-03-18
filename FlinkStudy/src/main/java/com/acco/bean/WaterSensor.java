package com.acco.bean;

import java.util.Objects;

/**
 * ClassName: WaterSensor
 * Description: None
 * Package: com.acco.bean
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-02-24 21:39
 */
public class WaterSensor {
    public String Id;
    public Long ts;
    public  Integer vc;
    // 空参的构造其一定要有
    public WaterSensor() {
    }

    public WaterSensor(String id, Long ts, Integer vc) {
        Id = id;
        this.ts = ts;
        this.vc = vc;
    }

    public String getId() {
        return Id;
    }

    public Long getTs() {
        return ts;
    }

    public Integer getVc() {
        return vc;
    }

    public void setId(String id) {
        Id = id;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public void setVc(Integer vc) {
        this.vc = vc;
    }

    @Override
    public String toString() {
        return "WaterSensor{" +
                "Id='" + Id + '\'' +
                ", ts=" + ts +
                ", vc=" + vc +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WaterSensor that = (WaterSensor) o;
        return Objects.equals(Id, that.Id) && Objects.equals(ts, that.ts) && Objects.equals(vc, that.vc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Id, ts, vc);
    }
}
