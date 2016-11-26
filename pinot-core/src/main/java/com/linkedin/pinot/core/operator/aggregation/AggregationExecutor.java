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
package com.linkedin.pinot.core.operator.aggregation;

import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import java.io.Serializable;
import java.util.List;
import java.util.Map;


/**
 * Interface for Aggregation executor, that executes all aggregation
 * functions (without group-bys). Aggregations are performed within a segment,
 * i.e. does not merge aggregation results from across different segments.
 */
public interface AggregationExecutor {

  /**
   * Initializations that need to be performed before process can be called.
   * Must be called before any of the other methods can be called.
   */
  void init();

  /**
   * Performs the actual aggregation on the given docId's of a segment.
   * Asserts that 'init' has been called before calling this method.
   *
   * @param projectionBlock Projection block on which to perform aggregation.
   */
  void aggregate(ProjectionBlock projectionBlock);

  /**
   * Post processing (if any) to be done after all docIdSets have been processed, and
   * before getResult can be called.
   * Must be called after all 'aggregation' calls are completed, and before 'getResult'.
   * Implementation to make sure that not calling it does not leak resources.
   */
  void finish();

  /**
   * Returns the result of aggregation.
   * Asserts that 'finish' has been called before calling getResult().
   *
   * @return
   */
  List<Serializable> getResult();
}
