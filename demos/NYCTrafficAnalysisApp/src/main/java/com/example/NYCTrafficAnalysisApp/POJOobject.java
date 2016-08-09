package com.example.NYCTrafficAnalysisApp;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.TimeZone;

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
        return "POJOobject [pickup=" + pickup + /*", dropoff=" + dropoff + */ ", cartype=" + cartype + ", time=" + time + ", fare=" + fare + "]";
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

    public long getTime()
    {
        return time;
    }

    public void setTime(String time) throws ParseException
    {
        SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yy HH:mm");
        //formatter.setTimeZone(TimeZone.getTimeZone("GMT-5"));
        Date parsedTime = formatter.parse(time);
        this.time = parsedTime.getTime();
    }

//    public void setTime(Date time)
//    {
//     this.time = time.toString();
//    }

}
