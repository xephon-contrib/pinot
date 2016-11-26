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
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.operator.docvaliterators.UnSortedSingleValueIterator;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;

public final class UnSortedSingleValueSet implements BlockValSet {
  final SingleColumnSingleValueReader sVReader;
  final ColumnMetadata columnMetadata;

  public UnSortedSingleValueSet(SingleColumnSingleValueReader sVReader,
      ColumnMetadata columnMetadata) {
    super();
    this.sVReader = sVReader;
    this.columnMetadata = columnMetadata;
  }

  @Override
  public <T> T getSingleValues() {
    throw new UnsupportedOperationException(
        "Reading a batch of values is not supported for unsorted single-value BlockValSet.");
  }

  @Override
  public <T> T getMultiValues() {
    throw new UnsupportedOperationException(
        "Reading a batch of values is not supported for unsorted single-value BlockValSet.");
  }

  @Override
  public BlockValIterator iterator() {
    return new UnSortedSingleValueIterator(sVReader, columnMetadata);
  }

  @Override
  public DataType getValueType() {
    return this.columnMetadata.getDataType();
  }

  @Override
  public void getDictionaryIds(int[] inDocIds, int inStartPos, int inDocIdsSize, int[] outDictionaryIds, int outStartPos) {
    sVReader.readValues(inDocIds, inStartPos, inDocIdsSize, outDictionaryIds, outStartPos);
  }

  @Override
  public int[] getDictionaryIds() {
    throw new UnsupportedOperationException(
        "Unsupported operation 'getDictionaryIds' for unsorted single-value BlockValSet.");
  }

  @Override
  public int getDictionaryIdForDocId(int docId) {
    throw new UnsupportedOperationException(
        "Reading value for a given docId not supported for unsorted single-value BlockValset.");
  }

  @Override
  public int getDictionaryIdsForDocId(int docId, int[] outputDictIds) {
    throw new UnsupportedOperationException(
        "Reading value for a given docId not supported for unsorted single-value BlockValset.");
  }
}
