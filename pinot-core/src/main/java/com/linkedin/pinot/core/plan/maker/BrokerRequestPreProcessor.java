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
package com.linkedin.pinot.core.plan.maker;

import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import java.util.List;


/**
 * The <code>BrokerRequestPreProcessor</code> class provides the utility to pre-process the {@link BrokerRequest}.
 * <p>After the pre-process, the {@link BrokerRequest} should not be further changed.
 */
public class BrokerRequestPreProcessor {
  private BrokerRequestPreProcessor() {
  }

  /**
   * Pre-process the {@link BrokerRequest}.
   * <p>Will apply the changes directly to the passed in object. Make a deep-copy before the pre-process if necessary.
   * <p>The following steps are performed:
   * <ul>
   *   <li>Rewrite 'fasthll' column name.</li>
   * </ul>
   *
   * @param segmentMetadata segment metadata.
   * @param brokerRequest broker request.
   */
  public static void preProcess(SegmentMetadata segmentMetadata, BrokerRequest brokerRequest) {
    if (brokerRequest.isSetAggregationsInfo()) {
      rewriteFastHllColumnName(segmentMetadata, brokerRequest);
    }
  }

  /**
   * Rewrite 'fasthll' column name.
   *
   * @param segmentMetadata segment metadata.
   * @param brokerRequest broker request.
   */
  private static void rewriteFastHllColumnName(SegmentMetadata segmentMetadata, BrokerRequest brokerRequest) {
    List<AggregationInfo> aggregationsInfo = brokerRequest.getAggregationsInfo();
    for (AggregationInfo aggregationInfo : aggregationsInfo) {
      if (aggregationInfo.getAggregationType().equalsIgnoreCase("fasthll")) {
        String column = aggregationInfo.getAggregationParams().get("column").trim();
        String hllDerivedColumn = segmentMetadata.getDerivedColumn(column, MetricFieldSpec.DerivedMetricType.HLL);
        if (hllDerivedColumn != null) {
          aggregationInfo.getAggregationParams().put("column", hllDerivedColumn);
        }
      }
    }
  }
}
