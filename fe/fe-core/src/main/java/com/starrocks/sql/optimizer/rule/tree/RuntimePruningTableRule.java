// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.optimizer.rule.tree;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.task.TaskContext;

public class RuntimePruningTableRule implements TreeRewriteRule {

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        return null;
    }

    private static class Visitor extends OptExpressionVisitor<OptExpression, Void> {

        @Override
        public OptExpression visit(OptExpression optExpression, Void context) {

            for (int i = 0; i < optExpression.arity(); ++i) {
                optExpression.inputAt(i).getOp().accept(this, optExpression.inputAt(i), null);
            }
            return optExpression;
        }
    }
}
