package com.example.NYCTrafficAnalysisApp;

/**
 * Created by aayushi on 7/7/16.
 */
public class POJOobject {

    private String pickup;
    private String dropoff;
    private int fare;

    @Override
    public String toString()
    {
        return "POJOobject [pickup=" + pickup + ", dropoff=" + dropoff + ", fare=" + fare + "]";
    }

    public String getPickup()
    {
        return pickup;
    }

    public void setPickup(String pickup)
    {
        this.pickup = pickup;
    }

    public String getDropoff()
    {
        return dropoff;
    }

    public void setDropoff(String dropoff)
    {
        this.dropoff = dropoff;
    }

    public int getFare()
    {
        return fare;
    }

    public void setFare(int fare)
    {
        this.fare = fare;
    }
}
