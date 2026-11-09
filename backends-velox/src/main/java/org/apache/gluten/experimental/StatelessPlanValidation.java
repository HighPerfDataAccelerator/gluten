/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.experimental;
import org.apache.gluten.validate.NativePlanValidationInfo;
import java.util.concurrent.atomic.AtomicLong;
public final class StatelessPlanValidation {
  private static final AtomicLong CALLS = new AtomicLong();
  private static final AtomicLong CHECKS = new AtomicLong();
  private StatelessPlanValidation() {}
  private static native NativePlanValidationInfo nativeValidate(byte[] plan, byte[] conf);
  public static NativePlanValidationInfo validate(byte[] plan, byte[] conf) {
    NativePlanValidationInfo out = nativeValidate(plan, conf);
    CALLS.incrementAndGet();
    return out;
  }
  public static void checkEquivalent(NativePlanValidationInfo expected, NativePlanValidationInfo actual) {
    if (expected.isSupported != actual.isSupported || !expected.fallbackInfo.equals(actual.fallbackInfo)) {
      throw new AssertionError("Stateless validation differs from original: " + expected.isSupported + ":" + expected.fallbackInfo + " vs " + actual.isSupported + ":" + actual.fallbackInfo);
    }
    CHECKS.incrementAndGet();
  }
  public static long validationCount() { return CALLS.get(); }
  public static long equivalenceCount() { return CHECKS.get(); }
}
