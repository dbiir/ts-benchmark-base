package cn.edu.ruc.adapter;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.ge.predix.timeseries.client.Client;
import com.ge.predix.timeseries.client.ClientFactory;
import com.ge.predix.timeseries.client.TenantContext;
import com.ge.predix.timeseries.client.TenantContextFactory;
import com.ge.predix.timeseries.exceptions.PredixTimeSeriesException;
import com.ge.predix.timeseries.model.builder.Aggregation;
import com.ge.predix.timeseries.model.builder.FilterBuilder;
import com.ge.predix.timeseries.model.builder.IngestionRequestBuilder;
import com.ge.predix.timeseries.model.builder.IngestionTag;
import com.ge.predix.timeseries.model.builder.QueryBuilder;
import com.ge.predix.timeseries.model.builder.QueryTag;
import com.ge.predix.timeseries.model.builder.TimeUnit;
import com.ge.predix.timeseries.model.datapoints.DataPoint;
import com.ge.predix.timeseries.model.datapoints.Quality;
import com.ge.predix.timeseries.model.response.IngestionResponse;
import com.ge.predix.timeseries.model.response.QueryResponse;

import cn.edu.ruc.base.Status;
import cn.edu.ruc.base.TsDataSource;
import cn.edu.ruc.base.TsParamConfig;
import cn.edu.ruc.base.TsQuery;
import cn.edu.ruc.base.TsWrite;

public class PredixAdater implements DBAdapter{

	@Override
	public void initDataSource(TsDataSource ds, TsParamConfig tspc) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object preWrite(TsWrite tsWrite) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status execWrite(Object write) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object preQuery(TsQuery tsQuery) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status execQuery(Object query) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void closeAdapter() {
		// TODO Auto-generated method stub
		
	}
	public static void main(String[] args) throws Exception {
		String path = PredixAdater.class.getResource("/").toString();     
		System.out.println("path = " + path);    
	 String queryuri="https://time-series-store-predix.run.aws-usw02-pr.ice.predix.io/v1/datapoints";
	 String queryAt="eyJhbGciOiJSUzI1NiIsImtpZCI6IlRyTnhGIiwidHlwIjoiSldUIn0.eyJqdGkiOiJkZTQ0MTlhYWQ3ZDg0ZDk1OTc4YTM2NTgwZWQ4ZjUwNiIsInN1YiI6InRzYm1vYXU2Iiwic2NvcGUiOlsidGltZXNlcmllcy56b25lcy42YzZlNGY0ZS00NWU1LTQ2OGItODM1ZS1kMmYyMzNjNmM2NzIucXVlcnkiLCJ0aW1lc2VyaWVzLnpvbmVzLmYxYTZiMGU1LTcxZDctNGI5My1hNzY5LTJmMTdjMzBiODIyMi5xdWVyeSIsInVhYS5ub25lIiwidGltZXNlcmllcy56b25lcy5mMWE2YjBlNS03MWQ3LTRiOTMtYTc2OS0yZjE3YzMwYjgyMjIuaW5nZXN0IiwidGltZXNlcmllcy56b25lcy42YzZlNGY0ZS00NWU1LTQ2OGItODM1ZS1kMmYyMzNjNmM2NzIuaW5nZXN0IiwidGltZXNlcmllcy56b25lcy5mMWE2YjBlNS03MWQ3LTRiOTMtYTc2OS0yZjE3YzMwYjgyMjIudXNlciJdLCJjbGllbnRfaWQiOiJ0c2Jtb2F1NiIsImNpZCI6InRzYm1vYXU2IiwiYXpwIjoidHNibW9hdTYiLCJncmFudF90eXBlIjoiY2xpZW50X2NyZWRlbnRpYWxzIiwicmV2X3NpZyI6IjQyODVkZWY4IiwiaWF0IjoxNTQyNTIxODQ5LCJleHAiOjE1NDI1NjUwNDksImlzcyI6Imh0dHBzOi8vMTAwODRkYmMtNTNjZS00ZDFlLTk0MDktYTZiYzhlNjgxZDY3LnByZWRpeC11YWEucnVuLmF3cy11c3cwMi1wci5pY2UucHJlZGl4LmlvL29hdXRoL3Rva2VuIiwiemlkIjoiMTAwODRkYmMtNTNjZS00ZDFlLTk0MDktYTZiYzhlNjgxZDY3IiwiYXVkIjpbInRpbWVzZXJpZXMuem9uZXMuZjFhNmIwZTUtNzFkNy00YjkzLWE3NjktMmYxN2MzMGI4MjIyIiwidGltZXNlcmllcy56b25lcy42YzZlNGY0ZS00NWU1LTQ2OGItODM1ZS1kMmYyMzNjNmM2NzIiLCJ0c2Jtb2F1NiJdfQ.DrKbAKBBxNg8Clj3eZZmkIn79Rcmrnid98WJ_FjfSVONFO7hKK0D4apKZxul1voeQiaYEKRKi5T85qH3BVAcgpPnHZUgmGB_D5NlGKyphvYPIoG73Pom-O_NdA6761PN82KArJXhN0RZfyne5_otACyOvegsSngUyEpPVTuvWxQ_pT7_OGZyGSwbpnd8rDqkEADG-9VmDxmftWwHqbUYfxQGV0Snfujx3kDSgMSOSpRO9yh_i4ffsYK2AXgfJ4Da7G1oW0VZiJ1Ah8pdRY0LsP44i2rlfsmkOVsLs_R0Fu0Fm4Z_ZBHd5fDnsJ7c_O16MymZyl7AGaYwqcRNm55M5g";
	 String ingestionUri="wss://gateway-predix-data-services.run.aws-usw02-pr.ice.predix.io/v1/stream/messages";
	 String ingestionAt="eyJhbGciOiJSUzI1NiIsImtpZCI6IlRyTnhGIiwidHlwIjoiSldUIn0.eyJqdGkiOiJkZTQ0MTlhYWQ3ZDg0ZDk1OTc4YTM2NTgwZWQ4ZjUwNiIsInN1YiI6InRzYm1vYXU2Iiwic2NvcGUiOlsidGltZXNlcmllcy56b25lcy42YzZlNGY0ZS00NWU1LTQ2OGItODM1ZS1kMmYyMzNjNmM2NzIucXVlcnkiLCJ0aW1lc2VyaWVzLnpvbmVzLmYxYTZiMGU1LTcxZDctNGI5My1hNzY5LTJmMTdjMzBiODIyMi5xdWVyeSIsInVhYS5ub25lIiwidGltZXNlcmllcy56b25lcy5mMWE2YjBlNS03MWQ3LTRiOTMtYTc2OS0yZjE3YzMwYjgyMjIuaW5nZXN0IiwidGltZXNlcmllcy56b25lcy42YzZlNGY0ZS00NWU1LTQ2OGItODM1ZS1kMmYyMzNjNmM2NzIuaW5nZXN0IiwidGltZXNlcmllcy56b25lcy5mMWE2YjBlNS03MWQ3LTRiOTMtYTc2OS0yZjE3YzMwYjgyMjIudXNlciJdLCJjbGllbnRfaWQiOiJ0c2Jtb2F1NiIsImNpZCI6InRzYm1vYXU2IiwiYXpwIjoidHNibW9hdTYiLCJncmFudF90eXBlIjoiY2xpZW50X2NyZWRlbnRpYWxzIiwicmV2X3NpZyI6IjQyODVkZWY4IiwiaWF0IjoxNTQyNTIxODQ5LCJleHAiOjE1NDI1NjUwNDksImlzcyI6Imh0dHBzOi8vMTAwODRkYmMtNTNjZS00ZDFlLTk0MDktYTZiYzhlNjgxZDY3LnByZWRpeC11YWEucnVuLmF3cy11c3cwMi1wci5pY2UucHJlZGl4LmlvL29hdXRoL3Rva2VuIiwiemlkIjoiMTAwODRkYmMtNTNjZS00ZDFlLTk0MDktYTZiYzhlNjgxZDY3IiwiYXVkIjpbInRpbWVzZXJpZXMuem9uZXMuZjFhNmIwZTUtNzFkNy00YjkzLWE3NjktMmYxN2MzMGI4MjIyIiwidGltZXNlcmllcy56b25lcy42YzZlNGY0ZS00NWU1LTQ2OGItODM1ZS1kMmYyMzNjNmM2NzIiLCJ0c2Jtb2F1NiJdfQ.DrKbAKBBxNg8Clj3eZZmkIn79Rcmrnid98WJ_FjfSVONFO7hKK0D4apKZxul1voeQiaYEKRKi5T85qH3BVAcgpPnHZUgmGB_D5NlGKyphvYPIoG73Pom-O_NdA6761PN82KArJXhN0RZfyne5_otACyOvegsSngUyEpPVTuvWxQ_pT7_OGZyGSwbpnd8rDqkEADG-9VmDxmftWwHqbUYfxQGV0Snfujx3kDSgMSOSpRO9yh_i4ffsYK2AXgfJ4Da7G1oW0VZiJ1Ah8pdRY0LsP44i2rlfsmkOVsLs_R0Fu0Fm4Z_ZBHd5fDnsJ7c_O16MymZyl7AGaYwqcRNm55M5g";
	 System.out.println(ingestionAt);
	 String zoneHeaderName="Predix-Zone-Id";
	 String zoneId="f1a6b0e5-71d7-4b93-a769-2f17c30b8222";
//	 String queryuri="1";
//	 String queryAt="2";
//	 String ingestionUri="3";
//	 String ingestionAt="4";
//	 String zoneHeaderName="Predix-Zone-Id";
//	 String zoneId="f1a6b0e5-71d7-4b93-a769-2f17c30b8222";
		TenantContext tenant = TenantContextFactory.createTenantContextFromProvidedProperties(queryuri	,queryAt,ingestionUri,ingestionAt, zoneHeaderName, zoneId);
//	 TenantContext tenant=TenantContextFactory.createTenantContextFromPropertiesFile("predix-timeseries.properties");	
	  QueryBuilder builder = QueryBuilder.createQuery() .withStartAbs(1427463525000L) .withEndAbs(1427483525000L)
				.addTags(
				QueryTag.Builder.createQueryTag()
				.withTagNames(Arrays.asList("ALT_SENSOR", "TEMP_SENSOR")) .withLimit(1000)
				.addAggregation(Aggregation.Builder.averageWithInterval(1, TimeUnit.HOURS)) .addFilters(FilterBuilder.getInstance()
				.addAttributeFilter("host", Arrays.asList("<host>")).build()) .addFilters(FilterBuilder.getInstance()
				.addAttributeFilter("type", Arrays.asList("<type>")).build()) .addFilters(FilterBuilder.getInstance()
				.addMeasurementFilter(FilterBuilder.Condition.GREATER_THAN_OR_EQUALS, Arrays.asList("23.1")).build())
				.addFilters(FilterBuilder.getInstance() .withQualitiesFilter(Arrays.asList(Quality.BAD, Quality.GOOD)).build())
				.build());
	  List<String> build = builder.build();
	  System.out.println(build);
	  Client queryClientForTenant = ClientFactory.queryClientForTenant(tenant);
				QueryResponse response = queryClientForTenant.queryAll(build);
				System.out.println(response);
//		 TenantContext tenant = TenantContextFactory.createIngestionTenantContextFromProvidedProperties(ingestionUri, ingestionAt, zoneHeaderName, zoneId);
//		Integer sensorValueAsInt = (int) Math.random();
//		Double sensorValueAsDouble = Math.random();
//		IngestionRequestBuilder ingestionBuilder = IngestionRequestBuilder.createIngestionRequest()
//		.withMessageId("<MessageID>") .addIngestionTag(IngestionTag.Builder.createIngestionTag()
//		.withTagName("TagName") .addDataPoints(
//		Arrays.asList(
//		new DataPoint(new Date().getTime(), sensorValueAsInt, Quality.GOOD),
//		new DataPoint(new Date().getTime(), sensorValueAsDouble,Quality.NOT_APPLICABLE),
//		new DataPoint(new Date().getTime(), "Bad Value", Quality.BAD),
//		new DataPoint(new Date().getTime(), null, Quality.UNCERTAIN) )
//		).addAttribute("AttributeKey", "AttributeValue") .addAttribute("AttributeKey2", "AttributeValue2") .build());
//		ingestionBuilder.build();
//		String json = ingestionBuilder.build().get(0);
//		System.out.println(json);
//		Client ingestionClientForTenant = ClientFactory.ingestionClientForTenant(tenant);
//		System.out.println("============"+ingestionClientForTenant);
//		IngestionResponse response1 = ingestionClientForTenant.ingest(json); 
//		String responseStr = response1.getMessageId() + response1.getStatusCode();
	}

}
