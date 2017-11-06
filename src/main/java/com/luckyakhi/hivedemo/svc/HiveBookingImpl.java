package com.luckyakhi.hivedemo.svc;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.jdbc.core.JdbcTemplate;

public class HiveBookingImpl implements HiveDAO{
	private JdbcTemplate jdbcTemplate;
	public Set<String> getPartitions(String databaseName,String tableName) {
		List<Map<String, Object>> resultSet=jdbcTemplate.queryForList("show partitions ?", databaseName+"."+tableName);
		return resultSet.stream().map(partitionMap -> (String)partitionMap.get("partitions")).
				collect(Collectors.toSet());
	}

}
