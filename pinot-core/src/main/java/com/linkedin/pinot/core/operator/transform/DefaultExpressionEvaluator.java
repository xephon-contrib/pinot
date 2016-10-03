/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.transform;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.common.request.transform.TransformFunction;
import com.linkedin.pinot.core.common.DataFetcher;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.aggregation.DataBlockCache;
import com.linkedin.pinot.common.request.transform.result.TransformResult;
import com.linkedin.pinot.core.operator.transform.result.DoubleArrayTransformResult;
import java.util.List;
import javax.annotation.Nonnull;


/**
 * Class for evaluating transform expression.
 */
public class DefaultExpressionEvaluator implements TransformExpressionEvaluator {

  private final DataBlockCache _dataBlockCache;
  private final TransformExpressionTree _expressionTree;
  private final MProjectionOperator _projectionOperator;

  private TransformResult _result;
  private int _length;

  boolean _inited;
  boolean _finished;

  /**
   * Constructor for the class.
   *  @param projectionOperator Projection Operator
   * @param expressionTree Expression tree to evaluate
   */
  public DefaultExpressionEvaluator(@Nonnull MProjectionOperator projectionOperator,
      @Nonnull TransformExpressionTree expressionTree) {

    _projectionOperator = projectionOperator;
    _dataBlockCache = new DataBlockCache(new DataFetcher(projectionOperator.getDataSourceMap()));
    _expressionTree = expressionTree;

    _inited = false;
    _finished = false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init() {
    _inited = true;
  }

  /**
   * {@inheritDoc}
   *
   * @param docIdSet Array of doc ids on which to evaluate the expression.
   * @param length Length of doc Ids to evaluate (array size could be larger).
   */
  @Override
  public void evaluate(int[] docIdSet, int length) {
    Preconditions.checkState(_inited, "Expression evaluator not initialized");

    _length = length;
    _dataBlockCache.initNewBlock(docIdSet, 0, length);
    _result = evaluateExpression(_expressionTree);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void finish() {
    Preconditions.checkState(_inited, "Expression evaluator not initialized");
    _finished = true;
  }

  /**
   * {@inheritDoc}
   * @return Returns the result of transform expression evaluation.
   */
  @Override
  public TransformResult getResult() {
    Preconditions.checkState(_finished, "Cannot get results from Expression evaluator before calling 'finish'");
    return _result;
  }

  /**
   * Helper (recursive) method that walks the expression tree bottom up evaluating
   * transforms at each level.
   *
   * @param expressionTree Expression tree to evaluate
   * @return Result of the expression transform
   */
  private TransformResult evaluateExpression(TransformExpressionTree expressionTree) {
    TransformFunction function = expressionTree.getTransform();

    if (function != null) {
      List<TransformExpressionTree> children = expressionTree.getChildren();
      Object[] transformArgs = new Object[children.size()];

      for (int i = 0; i < children.size(); i++) {
        transformArgs[i] = evaluateExpression(children.get(i)).getResultArray();
      }
      return function.transform(_length, transformArgs);
    } else {
      String column = expressionTree.getColumn();

      // TODO: Support non numeric columns.
      if (column != null) {
          return new DoubleArrayTransformResult(_dataBlockCache.getDoubleValueArrayForColumn(column));
      } else {
        throw new RuntimeException("Literals not supported in transforms yet");
      }
    }
  }
}
