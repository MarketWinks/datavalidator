package com.marketwinks.datavalidator.datavalidator.model;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;

public class uk_lse_5mins_livemarketprice {

	@Id
	public ObjectId _id;

	private String symbol;
	private java.time.LocalDateTime time;
	private String price;

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

}
