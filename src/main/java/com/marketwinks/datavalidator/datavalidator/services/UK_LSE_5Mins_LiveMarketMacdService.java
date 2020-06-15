
package com.marketwinks.datavalidator.datavalidator.services;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.marketwinks.datavalidator.datavalidator.model.uk_lse_5mins_livemarketmacd;
//import com.marketwinks.datavalidator.datavalidator.model.uk_lse_5mins_livemarketmacdjson;
import com.marketwinks.datavalidator.datavalidator.repository.UK_LSE_5Mins_LiveMarketMacdRepository;
//import com.marketwinks.datavalidator.datavalidator.repository.UK_LSE_5Mins_LiveMarketMacdjsonRepository;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

@RestController
@RequestMapping("/uk_lse_5mins_datavalidate")
public class UK_LSE_5Mins_LiveMarketMacdService {

	@Autowired
	private UK_LSE_5Mins_LiveMarketMacdRepository UK_LSE_5Mins_LiveMarketMacdRepository;

	@org.springframework.scheduling.annotation.Async
	@RequestMapping(value = "/{symbol}/{validationDate}/{startTime}/{endTime}/calc", method = RequestMethod.GET)
	public boolean UK_LSE_5Mins_Validator(@PathVariable String symbol, @PathVariable String validationDate,
			@PathVariable String startTime, @PathVariable String endTime) {
		System.gc();
		boolean execution_result = false;

		MongoClient mongoClient = MongoClients.create(
				"mongodb+srv://marketwinks:L9sS6oOAk1sHL0yi@aws-eu-west1-cluster-tszuq.mongodb.net/marketwinksdbprod?retryWrites=true");

		MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, "marketwinksdbprod");

		// List<uk_lse_5mins_livemarketmacd> MarketFeedswithnull =
		// MarketFeeds_full.stream()
		// .filter(a -> a.getSymbol().equals(symbol)).collect(Collectors.toList());

		try {

			Query query = new Query();
			query.addCriteria(Criteria.where("symbol").is(symbol));

			List<uk_lse_5mins_livemarketmacd> MarketFeeds_full = mongoTemplate.find(query,
					uk_lse_5mins_livemarketmacd.class);

			List<uk_lse_5mins_livemarketmacd> MarketFeeds_extract = new ArrayList<uk_lse_5mins_livemarketmacd>();
			Collections.sort(MarketFeeds_full, new SortbyLatestTime());
			int count = 0;
			for (int i = 0; i < MarketFeeds_full.size(); i++) {
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

				String formatDateTime = MarketFeeds_full.get(i).getTime().format(formatter);

				if (formatDateTime.equals(validationDate)) {
					MarketFeeds_extract.add(MarketFeeds_full.get(i));
				}

			}

			// System.out.println(MarketFeeds_extract.size());
			Collections.sort(MarketFeeds_extract, new SortbyLatestTime());
			//
			// System.out.println(MarketFeeds_extract.get(0).getTime());
			// System.out.println(MarketFeeds_extract.get(MarketFeeds_extract.size() -
			// 1).getTime());

			String starttime = validationDate + " " + startTime;
			String endtime = validationDate + " " + endTime;
			DateTimeFormatter hhmmformatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
			LocalDateTime dateTimestarttime = LocalDateTime.parse(starttime, hhmmformatter);
			LocalDateTime dateTimeendtime = LocalDateTime.parse(endtime, hhmmformatter);
			long minutes = dateTimestarttime.until(dateTimeendtime, ChronoUnit.MINUTES);
			int expectedCount = Math.toIntExact(minutes / 5 + 1);
			int actualCount = MarketFeeds_extract.size();
			int passcount = 0;

			System.out.println("REPORT FOR: " + symbol);
			System.out.println("REPORT DATE: " + validationDate);

			System.out.println("Expected count:" + expectedCount);
			System.out.println("Actual count:" + actualCount);
			if (expectedCount == actualCount) {
				System.out.println("Count good - passed");
				passcount++;
			} else {
				System.out.println("Count - failed");
			}

			String actualstarttime = MarketFeeds_extract.get(0).getTime()
					.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
			String actualendtime = MarketFeeds_extract.get(MarketFeeds_extract.size() - 1).getTime()
					.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));

			System.out.println("actual start time:" + actualstarttime);

			System.out.println("actual end time:" + actualendtime);

			if (actualstarttime.equals(starttime)) {
				System.out.println("start time validation passed");
			} else {
				System.out.println("start time validation failed");

			}

			if (actualendtime.equals(endtime)) {
				System.out.println("end time validation passed");
			} else {
				System.out.println("end time validation failed");

			}

			if (actualstarttime.equals(starttime) || actualendtime.equals(endtime)) {
				System.out.println("time validation passed");
				passcount++;
			} else {
				System.out.println("time validation failed");

			}

			List<String> MarketFeeds_timeextract = new ArrayList<String>();
			for (int i = 0; i < MarketFeeds_timeextract.size(); i++) {
				MarketFeeds_timeextract.add(
						MarketFeeds_extract.get(0).getTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")));
			}

			List<String> result = MarketFeeds_timeextract.stream()
					.filter(e -> Collections.frequency(MarketFeeds_timeextract, e) > 1).distinct()
					.collect(Collectors.toList());

			if (result.size() == 0) {
				System.out.println("No duplicates - passed");
				passcount++;

			} else {
				System.out.println("duplicates found - failed");

			}

			System.out.println("PASSED TOTAL TESTS:" + passcount);
			System.out.println("-------");

			execution_result = true;
		} catch (Exception e) {

			System.out.println(e);
		} finally {

			mongoClient.close();

			System.gc();

		}

		return execution_result;

	}

	private boolean checkfornull(String idofdocext) {
		boolean result = true;

		try {
			if (!idofdocext.isEmpty()) {
				result = false;
			}
			if (!idofdocext.equals(null)) {
				result = false;
			}

		} catch (Exception e) {
			result = true;
		}
		return result;
	}

}

class SortbyLatestTime implements Comparator<uk_lse_5mins_livemarketmacd> {
	public int compare(uk_lse_5mins_livemarketmacd a, uk_lse_5mins_livemarketmacd b) {
		return a.getTime().compareTo(b.getTime());
	}
}
