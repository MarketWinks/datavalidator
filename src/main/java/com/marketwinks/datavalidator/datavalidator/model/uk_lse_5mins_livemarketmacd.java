package com.marketwinks.datavalidator.datavalidator.model;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;

public class uk_lse_5mins_livemarketmacd {

	@Id
	public ObjectId _id;

	private String symbol;
	private java.time.LocalDateTime time;
	private String price;
	private String ema12;
	private String ema26;
	private String signal;
	private String macd;
	private String histogram;

	// ObjectId needs to be converted to string
	public String get_id() {
		return _id.toHexString();
	}

	public void set_id(ObjectId _id) {
		this._id = _id;
	}

	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}

	public java.time.LocalDateTime getTime() {
		return time;
	}

	public void setTime(java.time.LocalDateTime time) {
		this.time = time;
	}

	public String getPrice() {
		return price;
	}

	public void setPrice(String price) {
		this.price = price;
	}

	public String getSignal() {
		return signal;
	}

	public void setSignal(String signal) {
		this.signal = signal;
	}

	public String getMacd() {
		return macd;
	}

	public void setMacd(String macd) {
		this.macd = macd;
	}

	public String getHistogram() {
		return histogram;
	}

	public void setHistogram(String histogram) {
		this.histogram = histogram;
	}

	public String getEma12() {
		return ema12;
	}

	public void setEma12(String ema12) {
		this.ema12 = ema12;
	}

	public String getEma26() {
		return ema26;
	}

	public void setEma26(String ema26) {
		this.ema26 = ema26;
	}
}
