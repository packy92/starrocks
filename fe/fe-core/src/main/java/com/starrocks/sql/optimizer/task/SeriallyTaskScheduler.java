// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.task;

import com.google.common.base.Stopwatch;
import com.starrocks.common.Pair;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.Memo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Stack;
import java.util.concurrent.TimeUnit;

public class SeriallyTaskScheduler implements TaskScheduler {

    private static final Logger LOG = LogManager.getLogger(SeriallyTaskScheduler.class);

    private final Stack<OptimizerTask> tasks;

    private SeriallyTaskScheduler() {
        tasks = new Stack<>();
    }

    public static TaskScheduler create() {
        return new SeriallyTaskScheduler();
    }

    @Override
    public void executeTasks(TaskContext context) {
        long timeout = context.getOptimizerContext().getSessionVariable().getOptimizerExecuteTimeout();
        Stopwatch watch = context.getOptimizerContext().getTraceInfo().getStopwatch();
        while (!tasks.empty()) {
            if (watch.elapsed(TimeUnit.MILLISECONDS) > timeout) {
                // Should have at least one valid plan
                // group will be null when in rewrite phase
                // memo may be null for rule-based optimizer
                Memo memo = context.getOptimizerContext().getMemo();
                Group group = memo == null ? null : memo.getRootGroup();
                if (group == null || !group.hasBestExpression(context.getRequiredProperty())) {
                    throw new StarRocksPlannerException("StarRocks planner use long time " + timeout +
                            " ms in " + (group == null ? "logical" : "memo") + " phase, This probably because " +
                            "1. FE Full GC, " +
                            "2. Hive external table fetch metadata took a long time, " +
                            "3. The SQL is very complex. " +
                            "You could " +
                            "1. adjust FE JVM config, " +
                            "2. try query again, " +
                            "3. enlarge new_planner_optimize_timeout session variable",
                            ErrorType.INTERNAL_ERROR);
                }
                break;
            }
            OptimizerTask task = tasks.pop();
            context.getOptimizerContext().setTaskContext(context);
            long start = System.currentTimeMillis();
            task.execute();
            long costTime = System.currentTimeMillis() - start;
            if (task instanceof ApplyRuleTask) {
                context.map.put("apply", update(context.map.get("apply"), costTime));
            } else if (task instanceof DeriveStatsTask) {
                context.map.put("derive", update(context.map.get("derive"), costTime));
            } else if (task instanceof EnforceAndCostTask) {
                context.map.put("enforce", update(context.map.get("enforce"), costTime));
            } else if (task instanceof ExploreGroupTask) {
                context.map.put("explore", update(context.map.get("explore"), costTime));
            } else if (task instanceof OptimizeExpressionTask) {
                context.map.put("optimizeExp", update(context.map.get("optimizeExp"), costTime));
            } else if (task instanceof OptimizeGroupTask) {
                context.map.put("optimizeGroup", update(context.map.get("optimizeGroup"), costTime));
            }
        }
        System.out.println(context.map);
        LOG.info("each task cost time is: {}", context.map);
    }

    private Pair<Long, Long> update(Pair<Long, Long> pair, long time) {
        long first = pair.first + 1;
        long second = pair.second + time;
        return Pair.create(first, second);
    }

    @Override
    public void pushTask(OptimizerTask task) {
        tasks.push(task);
    }
}
