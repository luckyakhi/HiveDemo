package com.luckyakhi.hivedemo.svc;

import java.util.Set;

import com.luckyakhi.hivedemo.domain.PartitionInfo;

public interface PartitionExtractor {
	Set<PartitionInfo> extract(String databaseName,String tableName);
}
