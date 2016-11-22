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
package com.linkedin.pinot.core.operator.docvalsets;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.io.reader.impl.SortedForwardIndexReader;
import com.linkedin.pinot.core.io.reader.impl.SortedValueReaderContext;
import com.linkedin.pinot.core.operator.docvaliterators.SortedSingleValueIterator;

public final class SortedSingleValueSet implements BlockValSet {

  private SortedForwardIndexReader sVReader;

  public SortedSingleValueSet(SortedForwardIndexReader sVReader) {
    this.sVReader = sVReader;
  }

  @Override
  public <T> T getSingleValues() {
    throw new UnsupportedOperationException(
        "Reading a batch of values is not supported for sorted single -value BlockValSet.");
  }

  @Override
  public <T> T getMultiValues() {
    throw new UnsupportedOperationException(
        "Reading a batch of values is not supported for sorted single -value BlockValSet.");
  }

  @Override
  public BlockValIterator iterator() {
    return new SortedSingleValueIterator(sVReader);
  }

  @Override
  public DataType getValueType() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void getDictionaryIds(int[] inDocIds, int inStartPos, int inDocIdsSize, int[] outDictionaryIds, int outStartPos) {
    SortedValueReaderContext readerContext = sVReader.createContext();
    int endPos = inStartPos + inDocIdsSize;
    for (int iter = inStartPos; iter < endPos; iter++) {
      int row = inDocIds[iter];
      outDictionaryIds[outStartPos++] = sVReader.getInt(row, readerContext);
    }
  }

  @Override
  public int[] getDictionaryIds() {
    throw new UnsupportedOperationException(
        "Unsupported operation 'getDictionaryIds()' for sorted single-value BlockValSet.");
  }

  @Override
  public int getDictionaryIdForDocId(int docId) {
    throw new UnsupportedOperationException(
        "Reading value for a given docId is not supported for sorted single-value BlockValSet");
  }

  @Override
  public int getDictionaryIdsForDocId(int docId, int[] outputDictIds) {
    throw new UnsupportedOperationException(
        "Reading value for a given docId is not supported for sorted single-value BlockValSet");
  }
}
