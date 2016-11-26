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
package com.linkedin.pinot.core.operator.aggregation.groupby;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.operator.aggregation.AggregationFunctionContext;
import com.linkedin.pinot.core.operator.aggregation.ResultHolderFactory;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunction;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunctionFactory;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import com.linkedin.pinot.core.operator.docvalsets.ProjectionBlockValSet;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import java.util.List;


/**
 * This class implements group by aggregation.
 * It is optimized for performance, and uses the best possible algorithm/data-structure
 * for a given query based on the following parameters:
 * - Maximum number of group keys possible.
 * - Single/Multi valued columns.
 */
public class DefaultGroupByExecutor implements GroupByExecutor {

  private static final double GROUP_BY_TRIM_FACTOR = 0.9;
  private final int _numAggrFunc;
  private final int _numGroupsLimit;
  private final List<AggregationInfo> _aggregationInfoList;

  private  GroupKeyGenerator _groupKeyGenerator;
  private AggregationFunctionContext[] _aggrFuncContextArray;
  private GroupByResultHolder[] _resultHolderArray;
  private final String[] _groupByColumns;

  private int[] _docIdToSVGroupKey;
  private int[][] _docIdToMVGroupKey;

  private boolean _hasMultiValuedColumns = false;
  private boolean _inited = false; // boolean to ensure init() has been called.
  private boolean _finished = false; // boolean to ensure that finish() has been called.
  private boolean _groupByInited = false; // boolean for lazy creation of group-key generator etc.

  /**
   * Constructor for the class.
   *
   * @param aggregationInfoList Aggregation info from broker request
   * @param groupBy Group by from broker request
   * @param numGroupsLimit Limit on number of aggregation groups returned in the result
   */
  public DefaultGroupByExecutor(List<AggregationInfo> aggregationInfoList, GroupBy groupBy, int numGroupsLimit) {
    Preconditions.checkNotNull(aggregationInfoList);
    Preconditions.checkArgument(aggregationInfoList.size() > 0);
    Preconditions.checkNotNull(groupBy);

    List<String> groupByColumnList = groupBy.getColumns();
    _groupByColumns = groupByColumnList.toArray(new String[groupByColumnList.size()]);
    _numAggrFunc = aggregationInfoList.size();

    // TODO: revisit the trim factor. Usually the factor should be 5-10, and based on the 'TOP' limit.
    // When results are trimmed, drop bottom 10% of groups.
    _numGroupsLimit = (int) (GROUP_BY_TRIM_FACTOR * numGroupsLimit);

    _aggregationInfoList = aggregationInfoList;
  }

  /**
   * {@inheritDoc}
   * No-op for this implementation of GroupKeyGenerator. Most initialization happens lazily
   * in process(), as a projectionBlock is required to initialize group key generator, etc.
   */
  @Override
  public void init() {
    // Returned if already initialized.
    if (_inited) {
      return;
    }

    _inited = true;
  }

  /**
   * Process the provided set of docId's to perform the requested aggregation-group-by-operation.
   *
   * @param projectionBlock Projection block to process
   */
  @Override
  public void process(ProjectionBlock projectionBlock) {
    Preconditions
        .checkState(_inited, "Method 'process' cannot be called before 'init' for class " + getClass().getName());

    initGroupBy(projectionBlock);
    generateGroupKeysForBlock(projectionBlock);
    int capacityNeeded = _groupKeyGenerator.getCurrentGroupKeyUpperBound();

    for (int i = 0; i < _numAggrFunc; i++) {
      _resultHolderArray[i].ensureCapacity(capacityNeeded);
      aggregateColumn(projectionBlock, _aggrFuncContextArray[i], _resultHolderArray[i]);

      // Result holder limits the max number of group keys (default 100k), if the number of groups
      // exceeds beyond that limit, groups with lower values (as per sort order) are trimmed.
      // Once result holder trims those groups, the group key generator needs to purge them.
      int[] trimmedKeys = _resultHolderArray[i].trimResults();
      _groupKeyGenerator.purgeKeys(trimmedKeys);
    }
  }

  /**
   * Helper method to perform aggregation for a given column.
   *
   * @param projectionBlock Projection block to aggregate
   * @param aggrFuncContext Aggregation function context
   * @param resultHolder Holder for results of aggregation
   */
  @SuppressWarnings("ConstantConditions")
  private void aggregateColumn(ProjectionBlock projectionBlock, AggregationFunctionContext aggrFuncContext,
      GroupByResultHolder resultHolder) {
    AggregationFunction aggregationFunction = aggrFuncContext.getAggregationFunction();
    String[] aggrColumns = aggrFuncContext.getAggregationColumns();
    String aggrFuncName = aggregationFunction.getName();

    Preconditions.checkState(aggrColumns.length == 1);
    String aggrColumn = aggrColumns[0];
    boolean isAggrColumnSingleValue = true;

    BlockMetadata blockMetadata = projectionBlock.getDataBlock(aggrColumn).getMetadata();
    if (blockMetadata != null) {
      isAggrColumnSingleValue = blockMetadata.isSingleValue();
    }

    ProjectionBlockValSet blockValueSet = null;
    if (!aggrFuncName.equals(AggregationFunctionFactory.COUNT_AGGREGATION_FUNCTION)) {
      Block dataBlock = projectionBlock.getDataBlock(aggrColumn);
      blockValueSet = (ProjectionBlockValSet) dataBlock.getBlockValueSet();
    }

    int length = projectionBlock.getNumDocs();
    switch (aggrFuncName) {
      case AggregationFunctionFactory.COUNT_AGGREGATION_FUNCTION:
        if (_hasMultiValuedColumns) {
          aggregationFunction.aggregateGroupByMV(length, _docIdToMVGroupKey, resultHolder);
        } else {
          aggregationFunction.aggregateGroupBySV(length, _docIdToSVGroupKey, resultHolder);
        }
        break;

      case AggregationFunctionFactory.COUNT_MV_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.DISTINCTCOUNT_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.DISTINCTCOUNTHLL_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.DISTINCTCOUNT_MV_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.DISTINCTCOUNTHLL_MV_AGGREGATION_FUNCTION:
        Object hashCodeArray =
            (isAggrColumnSingleValue) ? blockValueSet.getSVHashCodeArray() : blockValueSet.getMVHashCodeArray();
        if (_hasMultiValuedColumns) {
          aggregationFunction.aggregateGroupByMV(length, _docIdToMVGroupKey, resultHolder, hashCodeArray);
        } else {
          aggregationFunction.aggregateGroupBySV(length, _docIdToSVGroupKey, resultHolder, hashCodeArray);
        }
        break;

      case AggregationFunctionFactory.FASTHLL_AGGREGATION_FUNCTION:
        Object stringArray =
            (isAggrColumnSingleValue) ? blockValueSet.getSingleValues() : blockValueSet.getMultiValues();
        if (_hasMultiValuedColumns) {
          aggregationFunction.aggregateGroupByMV(length, _docIdToMVGroupKey, resultHolder, (Object) stringArray);
        } else {
          aggregationFunction.aggregateGroupBySV(length, _docIdToSVGroupKey, resultHolder, (Object) stringArray);
        }
        break;

      default:
        Object valueArray =
            (isAggrColumnSingleValue) ? blockValueSet.getSingleValues() : blockValueSet.getMultiValues();
        if (_hasMultiValuedColumns) {
          aggregationFunction.aggregateGroupByMV(length, _docIdToMVGroupKey, resultHolder, (Object) valueArray);
        } else {
          aggregationFunction.aggregateGroupBySV(length, _docIdToSVGroupKey, resultHolder, (Object) valueArray);
        }
        break;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void finish() {
    Preconditions
        .checkState(_inited, "Method 'finish' cannot be called before 'init' for class " + getClass().getName());

    _finished = true;
  }

  /**
   * Return the final result of the aggregation-group-by operation.
   * This method should be called after all docIdSets have been 'processed'.
   *
   * @return Results of aggregation group by.
   */
  @Override
  public AggregationGroupByResult getResult() {
    Preconditions
        .checkState(_finished, "Method 'getResult' cannot be called before 'finish' for class " + getClass().getName());

    // If group by was not initialized (in case of no projection blocks), return null.
    if (!_groupByInited) {
      return null;
    }

    AggregationFunction.ResultDataType[] resultDataTypeArray = new AggregationFunction.ResultDataType[_numAggrFunc];
    for (int i = 0; i < _numAggrFunc; i++) {
      AggregationFunction aggregationFunction = _aggrFuncContextArray[i].getAggregationFunction();
      resultDataTypeArray[i] = aggregationFunction.getResultDataType();
    }
    return new AggregationGroupByResult(_groupKeyGenerator, _resultHolderArray, resultDataTypeArray);
  }

  /**
   * Generate group keys for the given docIdSet. For single valued columns, each docId has one group key,
   * but for multi-valued columns, each docId could have more than one group key.
   *
   * For SV keys: _docIdToSVGroupKey mapping is updated.
   * For MV keys: _docIdToMVGroupKey mapping is updated.
   *
   * @param projectionBlock Projection block for which to generate group keys
   */
  private void generateGroupKeysForBlock(ProjectionBlock projectionBlock) {
    if (_hasMultiValuedColumns) {
      _groupKeyGenerator.generateKeysForBlock(projectionBlock, _docIdToMVGroupKey);
    } else {
      _groupKeyGenerator.generateKeysForBlock(projectionBlock, _docIdToSVGroupKey);
    }
  }

  /**
   * Helper method to initialize result holder array.
   *
   * @param aggregationInfoList List of aggregation infos.
   * @param trimSize Trim size for group by keys
   * @param maxNumResults Maximum number of groups possible
   */
  private void initResultHolderArray(List<AggregationInfo> aggregationInfoList, int trimSize, int maxNumResults) {
    _aggrFuncContextArray = new AggregationFunctionContext[_numAggrFunc];
    _resultHolderArray = new GroupByResultHolder[_numAggrFunc];

    for (int i = 0; i < _numAggrFunc; i++) {
      AggregationInfo aggregationInfo = aggregationInfoList.get(i);
      String[] columns = aggregationInfo.getAggregationParams().get("column").trim().split(",");

      _aggrFuncContextArray[i] = new AggregationFunctionContext(aggregationInfo.getAggregationType(), columns);
      _resultHolderArray[i] =
          ResultHolderFactory.getGroupByResultHolder(_aggrFuncContextArray[i].getAggregationFunction(), maxNumResults,
              trimSize);
    }
  }

  /**
   * Allocate storage for docId to group keys mapping.
   */
  private void initDocIdToGroupKeyMap() {
    if (_hasMultiValuedColumns) {
      _docIdToMVGroupKey = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    } else {
      _docIdToSVGroupKey = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
  }

  /**
   * Initializes the following:
   * <p> - Group key generator. </p>
   * <p> - Result holders </p>
   * <p> - Re-usable storage (eg docId to group key mapping) </p>
   *
   * This is separate from init(), as this can only happen within process as projection block is
   * required to create group key generator.
   *
   * @param projectionBlock Projection block to group by.
   */
  private void initGroupBy(ProjectionBlock projectionBlock) {
    if (_groupByInited) {
      return;
    }
    _groupKeyGenerator = new DefaultGroupKeyGenerator(projectionBlock, _groupByColumns);
    int maxNumResults = _groupKeyGenerator.getGlobalGroupKeyUpperBound();
    _hasMultiValuedColumns = _groupKeyGenerator.hasMultiValueGroupByColumn();

    initResultHolderArray(_aggregationInfoList, _numGroupsLimit, maxNumResults);
    initDocIdToGroupKeyMap();
    _groupByInited = true;
  }
}
