// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.task;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.Memo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Stack;
import java.util.concurrent.TimeUnit;

public class SeriallyTaskScheduler implements TaskScheduler {

    private static final Logger LOG = LogManager.getLogger(SeriallyTaskScheduler.class);

    private final Stack<OptimizerTask> tasks;

    private static Map<String, Long> map = Maps.newHashMap();

    static {
        map.put("apply", 0L);
        map.put("derive", 0L);
        map.put("enforce", 0L);
        map.put("explore", 0L);
        map.put("optimizeExp", 0L);
        map.put("optimizeGroup", 0L);
    }

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
                map.put("apply", map.get("apply") + costTime);
            } else if (task instanceof DeriveStatsTask) {
                map.put("derive", map.get("derive") + costTime);
            } else if (task instanceof EnforceAndCostTask) {
                map.put("enforce", map.get("enforce") + costTime);
            } else if (task instanceof ExploreGroupTask) {
                map.put("explore", map.get("explore") + costTime);
            } else if (task instanceof OptimizeExpressionTask) {
                map.put("optimizeExp", map.get("optimizeExp") + costTime);
            } else if (task instanceof OptimizeGroupTask) {
                map.put("optimizeGroup", map.get("optimizeGroup") + costTime);
            }
        }
        System.out.println(map);
        LOG.info("each task cost time is: {}", map);
    }

    @Override
    public void pushTask(OptimizerTask task) {
        tasks.push(task);
    }
}
