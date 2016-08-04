package com.example.NYCTrafficAnalysisApp;

import java.util.Date;

/**
 * Created by aayushi on 7/7/16.
 */
public class POJOobject {

    private String pickup;
    private String dropoff;
    private String cartype;
    private int fare;
    private long time;

    @Override
    public String toString()
    {
        return "POJOobject [pickup=" + pickup + /*", dropoff=" + dropoff + */ ", cartype=" + cartype + ", fare=" + fare + ", time=" + time + "]";
    }

    public String getPickup()
    {
        return pickup;
    }

    public void setPickup(String pickup)
    {
        this.pickup = pickup;
    }

//    public String getDropoff()
//    {
//        return dropoff;
//    }
//
    public void setDropoff(String dropoff)
    {
        this.dropoff = dropoff;
    }

    public String getCartype()
    {
        return cartype;
    }

    public void setCartype(String cartype)
    {
        this.cartype = cartype;
    }

    public int getFare()
    {
        return fare;
    }

    public void setFare(int fare)
    {
        this.fare = fare;
    }

//    public void setTime(long time)
//    {
//        this.time = time;
//    }
//
    public long getTime()
    {
        return System.currentTimeMillis();
    }
}
