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
package com.linkedin.pinot.core.query.aggregation.function;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.operator.docidsets.ArrayBasedDocIdSet;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.io.Serializable;
import java.util.List;
import java.util.Locale;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This function will take a column and do sum on that.
 *
 */
public class SumAggregationFunction implements AggregationFunction<Double, Double> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SumAggregationFunction.class);

  private String _sumByColumn;
  static final int VECTOR_SIZE = 1000;
  private static final  ThreadLocal<int[]> DICTIONARY_ID_ARRAY = new ThreadLocal<int[]>() {
    @Override
    protected int[] initialValue() {
      return new int[VECTOR_SIZE];
    }
  };

  private static final ThreadLocal<double[]> DOC_VALUE_ARRAY = new ThreadLocal<double[]>() {
    @Override
    protected double[] initialValue() {
      return new double[VECTOR_SIZE];
    }
  };

  public SumAggregationFunction() {

  }

  @Override
  public void init(AggregationInfo aggregationInfo) {
    _sumByColumn = aggregationInfo.getAggregationParams().get("column");

  }

  @Override
  public Double aggregate(Block docIdSetBlock, Block[] blocks) {
    BlockDocIdSet blockDocIdSet = docIdSetBlock.getBlockDocIdSet();

    // for queries with high selectivity (10s of millions), iterative
    // approach to aggregation is quite slow. Hence switch to the
    // vectorized version. Some of the interface calls in
    // vectorizedAggregate are implemented only for ArrayBasedDocIdSet.
    // Hence, the if-else below. I'm pretty sure this method
    // is called for ArrayBasedDocIdSet only but don't want to take
    // RuntimeException
    assert blocks.length == 1;
    if ( blockDocIdSet instanceof  ArrayBasedDocIdSet) {
       return vectorizedAggregate((ArrayBasedDocIdSet) blockDocIdSet, blocks);
    } else {
      return iterativeAggregate(docIdSetBlock, blocks);
    }
  }


  public Double vectorizedAggregate(ArrayBasedDocIdSet docIdSet, Block[] blocks) {
    double ret = 0;
    Dictionary dictionaryReader = blocks[0].getMetadata().getDictionary();
    BlockValSet blockValueSet = blocks[0].getBlockValueSet();

    final int vectorSize = 1000;

    int[] dictIds = DICTIONARY_ID_ARRAY.get();
    double[] values = DOC_VALUE_ARRAY.get();

    int[] docIds = (int[]) docIdSet.getRaw();
    int docIdLength = docIdSet.size();
    int current = 0;
    while (current < docIdLength) {
      int pending = (docIdLength - current);
      int batchLimit = (pending > vectorSize) ? vectorSize : pending;
      blockValueSet.getDictionaryIds(docIds, current, batchLimit, dictIds, 0);
      dictionaryReader.readDoubleValues(dictIds, 0, batchLimit, values, 0);
      for (int vi = 0; vi < batchLimit; vi++) {
        ret += values[vi];
      }
      current += batchLimit;
    }
    return ret;
  }

  public Double iterativeAggregate(Block docIdSetBlock, Block[] blocks) {
    double ret = 0;
    int docId = 0;
    Dictionary dictionaryReader = blocks[0].getMetadata().getDictionary();
    BlockDocIdIterator docIdIterator = docIdSetBlock.getBlockDocIdSet().iterator();
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) blocks[0].getBlockValueSet().iterator();

    while ((docId = docIdIterator.next()) != Constants.EOF) {
      if (blockValIterator.skipTo(docId)) {
        int dictionaryIndex = blockValIterator.nextIntVal();
        if (dictionaryIndex != Dictionary.NULL_VALUE_INDEX) {
          ret += dictionaryReader.getDoubleValue(dictionaryIndex);
        }
      }
    }
    return ret;
  }

  @Override
  public Double aggregate(Double mergedResult, int docId, Block[] block) {
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) block[0].getBlockValueSet().iterator();
    if (blockValIterator.skipTo(docId)) {
      int dictionaryIndex = blockValIterator.nextIntVal();
      if (dictionaryIndex != Dictionary.NULL_VALUE_INDEX) {
        double value = block[0].getMetadata().getDictionary().getDoubleValue(dictionaryIndex);
        if (mergedResult == null) {
          return value;
        } else {
          return mergedResult + value;
        }
      } else {
        return mergedResult;
      }
    }
    return mergedResult;
  }

  @Override
  public List<Double> combine(List<Double> aggregationResultList, CombineLevel combineLevel) {
    double combinedResult = 0;
    for (double aggregationResult : aggregationResultList) {
      combinedResult += aggregationResult;
    }
    aggregationResultList.clear();
    aggregationResultList.add(combinedResult);
    return aggregationResultList;
  }

  @Override
  public Double combineTwoValues(Double aggregationResult0, Double aggregationResult1) {
    if (aggregationResult0 == null) {
      return aggregationResult1;
    }
    if (aggregationResult1 == null) {
      return aggregationResult0;
    }
    return aggregationResult0 + aggregationResult1;
  }

  @Override
  public Double reduce(List<Double> combinedResult) {
    double reducedResult = 0;
    for (double combineResult : combinedResult) {
      reducedResult += combineResult;
    }
    return reducedResult;
  }

  @Override
  public JSONObject render(Double finalAggregationResult) {
    try {
      if (finalAggregationResult == null) {
        finalAggregationResult = 0.0;
      }
      return new JSONObject().put("value", String.format(Locale.US, "%.5f", finalAggregationResult));
    } catch (JSONException e) {
      LOGGER.error("Caught exception while rendering to JSON", e);
      Utils.rethrowException(e);
      throw new AssertionError("Should not reach this");
    }
  }

  @Override
  public DataType aggregateResultDataType() {
    return DataType.DOUBLE;
  }

  @Override
  public String getFunctionName() {
    return "sum_" + _sumByColumn;
  }

  @Override
  public Serializable getDefaultValue() {
    return Double.valueOf(0);
  }

}
