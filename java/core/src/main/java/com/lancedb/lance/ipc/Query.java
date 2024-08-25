/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lancedb.lance.ipc;

import org.apache.arrow.util.Preconditions;

import java.util.Optional;

public class Query {
  public enum DistanceType {
    L2,
    Cosine,
    Dot,
    Hamming;
  }

  private final String column;
  private final float[] key;
  private final int k;
  private final int nprobes;
  private final Optional<Integer> ef;
  private final Optional<Integer> refineFactor;
  private final DistanceType metricType;
  private final boolean useIndex;

  private Query(Builder builder) {
    this.column = Preconditions.checkNotNull(builder.column, "Column must be set");
    Preconditions.checkArgument(!builder.column.isEmpty(), "Column must not be empty");
    this.key = Preconditions.checkNotNull(builder.key, "Key must be set");
    Preconditions.checkArgument(builder.k > 0, "K must be greater than 0");
    Preconditions.checkArgument(builder.nprobes > 0, "Nprobes must be greater than 0");
    this.k = builder.k;
    this.nprobes = builder.nprobes;
    this.ef = builder.ef;
    this.refineFactor = builder.refineFactor;
    this.metricType = Preconditions.checkNotNull(builder.metricType, "Metric type must be set");
    this.useIndex = builder.useIndex;
  }

  public String getColumn() {
    return column;
  }

  public float[] getKey() {
    return key;
  }

  public int getK() {
    return k;
  }

  public int getNprobes() {
    return nprobes;
  }

  public Optional<Integer> getEf() {
    return ef;
  }

  public Optional<Integer> getRefineFactor() {
    return refineFactor;
  }

  public String getMetricType() {
    return metricType.toString();
  }

  public boolean isUseIndex() {
    return useIndex;
  }

  public static class Builder {
    private String column;
    private float[] key;
    private int k;
    private int nprobes;
    private Optional<Integer> ef = Optional.empty();
    private Optional<Integer> refineFactor = Optional.empty();
    private DistanceType metricType;
    private boolean useIndex = true;

    /**
     * Sets the column to be searched.
     *
     * @param column The name of the column to search in.
     * @return The Builder instance for method chaining.
     */
    public Builder setColumn(String column) {
      this.column = column;
      return this;
    }

    /**
     * Sets the vector to be searched.
     *
     * @param key The search vector.
     * @return The Builder instance for method chaining.
     */
    public Builder setKey(float[] key) {
      this.key = key;
      return this;
    }

    /**
     * Sets the number of top results to return.
     *
     * @param k The number of top results to return.
     * @return The Builder instance for method chaining.
     */
    public Builder setK(int k) {
      this.k = k;
      return this;
    }

    /**
     * Sets the number of probes to load and search.
     *
     * @param nprobes The number of probes.
     * @return The Builder instance for method chaining.
     */
    public Builder setNprobes(int nprobes) {
      this.nprobes = nprobes;
      return this;
    }

    /**
     * Sets the number of candidates to reserve while searching.
     * This is an optional parameter for HNSW related index types.
     *
     * @param ef The number of candidates to reserve.
     * @return The Builder instance for method chaining.
     */
    public Builder setEf(int ef) {
      this.ef = Optional.of(ef);
      return this;
    }

    /**
     * Sets the refine factor for applying a refine step.
     *
     * @param refineFactor The refine factor.
     * @return The Builder instance for method chaining.
     */
    public Builder setRefineFactor(int refineFactor) {
      this.refineFactor = Optional.of(refineFactor);
      return this;
    }

    /**
     * Sets the distance metric type.
     *
     * @param metricType The DistanceType to use for the query.
     * @return The Builder instance for method chaining.
     */
    public Builder setMetricType(DistanceType metricType) {
      this.metricType = metricType;
      return this;
    }

    /**
     * Sets whether to use an ANN index if available.
     *
     * @param useIndex True to use the index, false otherwise.
     * @return The Builder instance for method chaining.
     */
    public Builder setUseIndex(boolean useIndex) {
      this.useIndex = useIndex;
      return this;
    }

    /**
     * Builds the Query object.
     *
     * @return A new immutable Query instance.
     * @throws IllegalStateException if any required fields are not set or have invalid values.
     */
    public Query build() {
      return new Query(this);
    }
  }
}