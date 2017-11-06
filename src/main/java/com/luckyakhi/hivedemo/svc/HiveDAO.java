package com.luckyakhi.hivedemo.svc;

import java.util.Set;

public interface HiveDAO {
	public Set<String> getPartitions(String databaseName,String tableName);
}
