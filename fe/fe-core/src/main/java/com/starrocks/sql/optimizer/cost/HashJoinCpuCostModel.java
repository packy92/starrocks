// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.cost;

import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;

import java.util.List;

public class HashJoinCpuCostModel {

    private final Statistics leftStatistics;

    private final Statistics rightStatistics;

    private final List<BinaryPredicateOperator> eqConditions;

    public HashJoinCpuCostModel(Statistics leftStatistics, Statistics rightStatistics, List<BinaryPredicateOperator> eqConditions) {
        this.leftStatistics = leftStatistics;
        this.rightStatistics = rightStatistics;
        this.eqConditions = eqConditions;

    }

    public double getCpuCost() {
        double buildCost = rightStatistics.getOutputRowCount() * getAvgBuildCost();
        double probeCost = leftStatistics.getOutputRowCount() * getAvgProbeCost();
        return buildCost + probeCost;
    }

    private double getAvgProbeCost() {
        double rowCount = rightStatistics.getOutputRowCount();
        double distinctRowCount = getRightTableColStats().getDistinctValuesCount();
        return rowCount/distinctRowCount;
    }

    private double getAvgBuildCost() {
        double rowCount = rightStatistics.getOutputRowCount();
        double distinctRowCount = getRightTableColStats().getDistinctValuesCount();
        return Math.max(1, rowCount/distinctRowCount/2);
    }


    private ColumnStatistic getRightTableColStats() {
        List<ScalarOperator> operands = eqConditions.get(0).getChildren();
        ColumnStatistic result = null;
        for (ScalarOperator operand : operands) {
            ColumnRefOperator columnRefOperator = (ColumnRefOperator) operand;
            if (rightStatistics.getUsedColumns().contains(columnRefOperator)) {
                result = rightStatistics.getColumnStatistic(columnRefOperator);
                break;
            }
        }
        return result;
    }


}
