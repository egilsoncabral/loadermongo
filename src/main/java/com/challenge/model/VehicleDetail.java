package com.challenge.model;

import java.math.BigInteger;
import java.time.LocalDate;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.data.annotation.Id;

@Data
@AllArgsConstructor
public class VehicleDetail{

    @Id
    private BigInteger _id;
    private long timestamp;
    private String lineId;
    private String direction;
    private String journeyPatternId;
    private LocalDate timeFrame;
    private String vehicleJourneyId;
    private String operator;
    private boolean congestion;
    private String longitude;
    private String latitude;
    private Double delay;
    private String blockId;
    private String vehicleId;
    private String stopId;
    private boolean atStop;

    public VehicleDetail(){

    }


    public void setTimeFrame(String timeFrame) {
    	this.timeFrame = LocalDate.parse(timeFrame);
    }

}
