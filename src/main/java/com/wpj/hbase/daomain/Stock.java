/*
 * Copyright (c) 2016 - 1 - 6  10 : 56 :12
 * @author wupeiji It will be
 * @Email wpjlovehome@gmail.com
 */

package com.wpj.hbase.daomain;

import java.util.Date;

/**
 * Name：Stock
 * Time：2016/1/6 22:56
 * author：WPJ587
 * description：股票实体类
 **/

public class Stock {
    private String rowKey;//行键
    private Date date;//日期
    private double startPrice;//开盘价
    private double highestPrice;//最高价
    private double overPrice; //收盘价
    private double lowestPrice;//最低价
    private long tradingVolume;//交易股
    private long tradingTotalMoney;//交易总额
    private String companyName;

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public double getStartPrice() {
        return startPrice;
    }

    public void setStartPrice(double startPrice) {
        this.startPrice = startPrice;
    }

    public double getHighestPrice() {
        return highestPrice;
    }

    public void setHighestPrice(double highestPrice) {
        this.highestPrice = highestPrice;
    }

    public double getOverPrice() {
        return overPrice;
    }

    public void setOverPrice(double overPrice) {
        this.overPrice = overPrice;
    }

    public double getLowestPrice() {
        return lowestPrice;
    }

    public void setLowestPrice(double lowestPrice) {
        this.lowestPrice = lowestPrice;
    }

    public long getTradingVolume() {
        return tradingVolume;
    }

    public void setTradingVolume(long tradingVolume) {
        this.tradingVolume = tradingVolume;
    }

    public long getTradingTotalMOney() {
        return tradingTotalMoney;
    }

    public void setTradingTotalMOney(long tradingTotalMOney) {
        this.tradingTotalMoney = tradingTotalMOney;
    }

    public Stock(String rowKey, Date date, double startPrice, double highestPrice,
                 double overPrice, double lowestPrice, long tradingVolume,
                 long tradingTotalMOney, String companyName) {
        this.rowKey = rowKey;
        this.date = date;
        this.startPrice = startPrice;
        this.highestPrice = highestPrice;
        this.overPrice = overPrice;
        this.lowestPrice = lowestPrice;
        this.tradingVolume = tradingVolume;
        this.tradingTotalMoney = tradingTotalMOney;
        this.companyName = companyName;
    }
}
