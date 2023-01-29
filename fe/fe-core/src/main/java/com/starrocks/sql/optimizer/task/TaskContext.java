// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.task;

import com.google.common.collect.Maps;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;

import java.util.Collections;
import java.util.List;
import java.util.Map;

// The context for optimizer task
public class TaskContext {
    private final OptimizerContext optimizerContext;
    private final PhysicalPropertySet requiredProperty;
    private ColumnRefSet requiredColumns;
    private double upperBoundCost;
    private List<LogicalOlapScanOperator> allScanOperators;

    public Map<String, Pair<Long, Long>> map = Maps.newHashMap();

    public TaskContext(OptimizerContext context,
                       PhysicalPropertySet physicalPropertySet,
                       ColumnRefSet requiredColumns,
                       double cost) {
        this.optimizerContext = context;
        this.requiredProperty = physicalPropertySet;
        this.requiredColumns = requiredColumns;
        this.upperBoundCost = cost;
        this.allScanOperators = Collections.emptyList();
        map.put("apply", Pair.create(0L, 0L));
        map.put("derive", Pair.create(0L, 0L));
        map.put("enforce", Pair.create(0L, 0L));
        map.put("explore", Pair.create(0L, 0L));
        map.put("optimizeExp", Pair.create(0L, 0L));
        map.put("optimizeGroup", Pair.create(0L, 0L));
    }

    public OptimizerContext getOptimizerContext() {
        return optimizerContext;
    }

    public double getUpperBoundCost() {
        return upperBoundCost;
    }

    public PhysicalPropertySet getRequiredProperty() {
        return requiredProperty;
    }

    public ColumnRefSet getRequiredColumns() {
        return requiredColumns;
    }

    public void setRequiredColumns(ColumnRefSet requiredColumns) {
        this.requiredColumns = requiredColumns;
    }

    public void setUpperBoundCost(double upperBoundCost) {
        this.upperBoundCost = upperBoundCost;
    }

    public void setAllScanOperators(List<LogicalOlapScanOperator> allScanOperators) {
        this.allScanOperators = allScanOperators;
    }

    public List<LogicalOlapScanOperator> getAllScanOperators() {
        return allScanOperators;
    }
}
