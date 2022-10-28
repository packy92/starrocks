// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.cost;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.statistics.Statistics;

import java.util.List;

public class HashJoinCostModel {

    private static final String EMPTY = "EMPTY";

    private static final String BROADCAST = "BROADCAST";

    private static final String SHUFFLE = "SHUFFLE";

    private static final int BOTTOM_NUMBER = 10000;

    private final Statistics leftStatistics;

    private final Statistics rightStatistics;

    private final ExpressionContext context;

    private final List<PhysicalPropertySet> inputProperties;


    public HashJoinCostModel(ExpressionContext context, List<PhysicalPropertySet> inputProperties) {
        this.context = context;
        this.leftStatistics = context.getChildStatistics(0);
        this.rightStatistics = context.getChildStatistics(1);
        this.inputProperties = inputProperties;
    }

    public double getCpuCost() {
        String execMode = deriveJoinExecMode();
        double buildCost;
        double probeCost;
        double leftOutput = leftStatistics.getOutputSize(context.getChildOutputColumns(0));
        double rightOutput = rightStatistics.getOutputSize(context.getChildOutputColumns(1));
        int beNum = Math.max(1, ConnectContext.get().getAliveBackendNumber());
        switch (execMode) {
            case BROADCAST:
                buildCost = rightOutput;
                probeCost = leftOutput * getAvgProbeCost();
                break;
            case SHUFFLE:
                buildCost = rightOutput / beNum;
                probeCost = leftOutput * getAvgProbeCost();
                break;
            default:
                buildCost = rightOutput;
                probeCost = leftOutput;
        }
        return buildCost + probeCost;
    }

    public double getMemCost() {
        String execMode = deriveJoinExecMode();
        double rightOutput = rightStatistics.getOutputSize(context.getChildOutputColumns(1));
        double memCost;
        int beNum = Math.max(1, ConnectContext.get().getAliveBackendNumber());
        switch (execMode) {
            case BROADCAST:
                memCost = rightOutput * beNum;
                break;
            default:
                memCost = rightOutput;
        }
        return memCost;
    }

    private double getAvgProbeCost() {
        String execMode = deriveJoinExecMode();
        double rowCount = rightStatistics.getOutputRowCount();
        double degradeRation;
        int beNum = Math.max(1, ConnectContext.get().getAliveBackendNumber());
        switch (execMode) {
            case BROADCAST:
                degradeRation = Math.max(1, Math.log10(rowCount) / Math.log10(BOTTOM_NUMBER));
                break;
            default:
                degradeRation = Math.max(1, Math.log10(rowCount / beNum) / Math.log10(BOTTOM_NUMBER));
        }
        return degradeRation;
    }


    private String deriveJoinExecMode() {
        if (inputProperties == null) {
            return EMPTY;
        } else if (inputProperties.get(1).getDistributionProperty().isBroadcast()) {
            return BROADCAST;
        } else {
            return SHUFFLE;
        }
    }
}
