package com.zxm.st;

import java.io.*;

public class StockQuotationInfo implements Serializable {

	public String getStockCode() {
		return stockCode;
	}

	public void setStockCode(String stockCode) {
		this.stockCode = stockCode;
	}

	public String getStockName() {
		return stockName;
	}

	public void setStockName(String stockName) {
		this.stockName = stockName;
	}

	public long getTradeTime() {
		return tradeTime;
	}

	public void setTradeTime(long tradeTime) {
		this.tradeTime = tradeTime;
	}

	public float getPreClosePrice() {
		return preClosePrice;
	}

	public void setPreClosePrice(float preClosePrice) {
		this.preClosePrice = preClosePrice;
	}

	public float getOpenPrice() {
		return openPrice;
	}

	public void setOpenPrice(float openPrice) {
		this.openPrice = openPrice;
	}

	public float getCurrentPrice() {
		return currentPrice;
	}

	public void setCurrentPrice(float currentPrice) {
		this.currentPrice = currentPrice;
	}

	public float getHighPrice() {
		return highPrice;
	}

	public void setHighPrice(float highPrice) {
		this.highPrice = highPrice;
	}

	public float getLowPrice() {
		return lowPrice;
	}

	public void setLowPrice(float lowPrice) {
		this.lowPrice = lowPrice;
	}

	private static final long serialVersionUID = 1L;

	private String stockCode;
	private String stockName;
	private long tradeTime;
	private float preClosePrice;
	private float openPrice;
	private float currentPrice;
	private float highPrice;
	private float lowPrice;

	@Override
	public String toString() {
		return "["+this.stockCode+"|"+this.stockName+"|"+this.tradeTime+"|"+preClosePrice+"|"
				+openPrice+"|"+currentPrice+"|"+highPrice+"|"+lowPrice+"]";
		
	}

}
